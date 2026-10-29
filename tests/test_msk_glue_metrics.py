from datetime import UTC, datetime

import boto3
import pytest
from botocore.stub import Stubber
from moto import mock_aws

from fulcrum_mcp.errors import AccessDeniedError, ConfigError, InputValidationError, NotFoundError
from fulcrum_mcp.glue_registry import GlueRegistry
from fulcrum_mcp.metrics import Metrics
from fulcrum_mcp.msk_client import MskClient, endpoint_label

from .conftest import BOOTSTRAP, CLUSTER_ARN, describe_cluster_response


def stubbed(service: str):
    client = boto3.client(service, region_name="us-east-1")
    stub = Stubber(client)
    stub.activate()
    return client, stub


# -- MSK ------------------------------------------------------------------------------------


@mock_aws
def test_list_clusters_with_moto():
    kafka = boto3.client("kafka", region_name="us-east-1")
    kafka.create_cluster_v2(
        ClusterName="zeta",
        Provisioned={
            "BrokerNodeGroupInfo": {"InstanceType": "kafka.m5.large", "ClientSubnets": ["subnet-1", "subnet-2"]},
            "KafkaVersion": "3.6.0",
            "NumberOfBrokerNodes": 2,
        },
    )
    kafka.create_cluster_v2(ClusterName="alpha", Serverless={"VpcConfigs": [{"SubnetIds": ["subnet-1"]}]})
    clusters = MskClient(kafka).list_clusters()
    assert [c.name for c in clusters] == ["alpha", "zeta"]
    assert clusters[0].cluster_type == "SERVERLESS"


def test_describe_cluster_and_endpoints():
    client, stub = stubbed("kafka")
    stub.add_response("describe_cluster_v2", describe_cluster_response(), {"ClusterArn": CLUSTER_ARN})
    stub.add_response("get_bootstrap_brokers", BOOTSTRAP, {"ClusterArn": CLUSTER_ARN})
    info = MskClient(client).describe_cluster(CLUSTER_ARN)
    assert info.kafka_version == "3.6.0" and info.broker_count == 3
    assert info.broker_instance_type == "kafka.m7g.large"
    assert info.auth_types == ["SASL/IAM"]
    assert "BootstrapBrokerStringSaslIam" in info.endpoints
    assert endpoint_label("BootstrapBrokerStringSaslIam") == "SASL/IAM (private)"
    assert endpoint_label("Other") == "Other"


def test_iam_bootstrap_prefers_private_then_public():
    client, stub = stubbed("kafka")
    stub.add_response("get_bootstrap_brokers", BOOTSTRAP, {"ClusterArn": CLUSTER_ARN})
    stub.add_response(
        "get_bootstrap_brokers", {"BootstrapBrokerStringPublicSaslIam": "pub:9198"}, {"ClusterArn": CLUSTER_ARN}
    )
    stub.add_response("get_bootstrap_brokers", {"BootstrapBrokerStringTls": "tls:9094"}, {"ClusterArn": CLUSTER_ARN})
    msk = MskClient(client)
    assert msk.iam_bootstrap(CLUSTER_ARN).endswith(":9098,b-2.orders:9098")
    assert msk.iam_bootstrap(CLUSTER_ARN) == "pub:9198"
    with pytest.raises(ConfigError, match="IAM"):
        msk.iam_bootstrap(CLUSTER_ARN)


def test_msk_errors():
    client, stub = stubbed("kafka")
    stub.add_client_error("describe_cluster_v2", "NotFoundException", "missing")
    stub.add_client_error("list_clusters_v2", "AccessDeniedException", "denied")
    stub.add_client_error("get_bootstrap_brokers", "AccessDeniedException", "denied")
    msk = MskClient(client)
    with pytest.raises(NotFoundError):
        msk.describe_cluster(CLUSTER_ARN)
    with pytest.raises(AccessDeniedError, match="kafka:ListClustersV2"):
        msk.list_clusters()
    with pytest.raises(AccessDeniedError, match="kafka:GetBootstrapBrokers"):
        msk.bootstrap_endpoints(CLUSTER_ARN)


# -- Glue -----------------------------------------------------------------------------------


@mock_aws
def test_registries_and_schema_with_moto():
    glue = boto3.client("glue", region_name="us-east-1")
    glue.create_registry(RegistryName="prod", Description="production")
    glue.create_schema(
        RegistryId={"RegistryName": "prod"},
        SchemaName="orders",
        DataFormat="AVRO",
        Compatibility="BACKWARD",
        SchemaDefinition='{"type":"record","name":"O","fields":[{"name":"id","type":"string"}]}',
    )
    registry = GlueRegistry(glue)
    assert [r.name for r in registry.list_registries()] == ["prod"]
    assert registry.get_schema("orders", "prod")["Compatibility"] == "BACKWARD"
    version = registry.get_version("orders", "prod", None)
    assert version.version == 1 and version.data_format == "AVRO"
    assert '"id"' in registry.get_version("orders", "prod", 1).definition


def _schemas_page(registry, *names):
    return {"Schemas": [{"RegistryName": registry, "SchemaName": n, "SchemaStatus": "AVAILABLE"} for n in names]}


def test_list_schemas_paginates_across_registries():
    client, stub = stubbed("glue")
    stub.add_response("list_registries", {"Registries": [{"RegistryName": "b"}], "NextToken": "t"}, {})
    stub.add_response("list_registries", {"Registries": [{"RegistryName": "a"}]}, {"NextToken": "t"})
    # registries are sorted, so "a" is listed first
    stub.add_response(
        "list_schemas", {**_schemas_page("a", "z1"), "NextToken": "n"}, {"RegistryId": {"RegistryName": "a"}}
    )
    stub.add_response(
        "list_schemas", _schemas_page("a", "orders"), {"RegistryId": {"RegistryName": "a"}, "NextToken": "n"}
    )
    stub.add_response("list_schemas", _schemas_page("b", "orders-v2"), {"RegistryId": {"RegistryName": "b"}})
    # The Stubber is order-sensitive, so page through each registry sequentially here.
    reg = GlueRegistry(client)
    names = [r.name for r in reg.list_registries()]
    assert names == ["a", "b"]
    schemas = [*reg._list_schemas_in("a"), *reg._list_schemas_in("b")]
    assert [s["SchemaName"] for s in schemas] == ["z1", "orders", "orders-v2"]


def test_list_schemas_with_prefix_and_default_registry():
    client, stub = stubbed("glue")
    stub.add_response(
        "list_schemas", _schemas_page("prod", "orders", "payments"), {"RegistryId": {"RegistryName": "prod"}}
    )
    schemas = GlueRegistry(client, default_registry="prod").list_schemas(prefix="ord")
    assert [s.name for s in schemas] == ["orders"]


def test_resolve_registry():
    client, stub = stubbed("glue")
    reg = GlueRegistry(client)
    assert reg.resolve_registry("x", "given") == "given"
    assert GlueRegistry(client, "default").resolve_registry("x", None) == "default"

    stub.add_response("list_registries", {"Registries": [{"RegistryName": "a"}]}, {})
    stub.add_response("list_schemas", _schemas_page("a", "orders"), {"RegistryId": {"RegistryName": "a"}})
    assert reg.resolve_registry("orders", None) == "a"

    stub.add_response("list_registries", {"Registries": [{"RegistryName": "a"}]}, {})
    stub.add_response("list_schemas", _schemas_page("a", "orders"), {"RegistryId": {"RegistryName": "a"}})
    with pytest.raises(NotFoundError):
        reg.resolve_registry("missing", None)


def test_resolve_registry_ambiguous(monkeypatch):
    from fulcrum_mcp.models import SchemaSummary

    client, _ = stubbed("glue")
    reg = GlueRegistry(client)
    monkeypatch.setattr(
        reg, "list_schemas", lambda *a, **k: [SchemaSummary(registry=r, name="orders") for r in ("a", "b")]
    )
    with pytest.raises(InputValidationError, match="several registries"):
        reg.resolve_registry("orders", None)


def test_describe_schema_and_versions():
    client, stub = stubbed("glue")
    sid = {"RegistryName": "prod", "SchemaName": "orders"}
    stub.add_response(
        "get_schema",
        {
            "SchemaName": "orders",
            "RegistryName": "prod",
            "DataFormat": "AVRO",
            "Compatibility": "BACKWARD",
            "LatestSchemaVersion": 2,
            "SchemaStatus": "AVAILABLE",
            "Description": "Order events",
        },
        {"SchemaId": sid},
    )
    stub.add_response(
        "list_schema_versions",
        {"Schemas": [{"VersionNumber": 2, "Status": "AVAILABLE"}], "NextToken": "n"},
        {"SchemaId": sid},
    )
    stub.add_response(
        "list_schema_versions",
        {"Schemas": [{"VersionNumber": 1, "Status": "AVAILABLE", "CreatedTime": "2026-01-01"}]},
        {"SchemaId": sid, "NextToken": "n"},
    )
    stub.add_response(
        "get_schema_version",
        {"VersionNumber": 2, "DataFormat": "AVRO", "SchemaDefinition": "{}", "Status": "AVAILABLE"},
        {"SchemaId": sid, "SchemaVersionNumber": {"LatestVersion": True}},
    )
    detail = GlueRegistry(client).describe("orders", "prod", None)
    assert detail.summary.compatibility == "BACKWARD"
    assert [v.version for v in detail.versions] == [1, 2]
    assert detail.current.version == 2


def test_glue_errors_name_actions():
    client, stub = stubbed("glue")
    stub.add_client_error("list_registries", "AccessDeniedException")
    stub.add_client_error("get_schema", "EntityNotFoundException")
    stub.add_client_error("list_schema_versions", "AccessDeniedException")
    stub.add_client_error("get_schema_version", "AccessDeniedException")
    stub.add_client_error("list_schemas", "AccessDeniedException")
    reg = GlueRegistry(client)
    with pytest.raises(AccessDeniedError, match="glue:ListRegistries"):
        reg.list_registries()
    with pytest.raises(NotFoundError):
        reg.get_schema("s", "r")
    with pytest.raises(AccessDeniedError, match="glue:ListSchemaVersions"):
        reg.list_versions("s", "r")
    with pytest.raises(AccessDeniedError, match="glue:GetSchemaVersion"):
        reg.get_version("s", "r", 3)
    with pytest.raises(AccessDeniedError, match="glue:ListSchemas"):
        reg.list_schemas("r")


# -- CloudWatch -----------------------------------------------------------------------------


def test_topic_throughput_sums_brokers_and_follows_pages():
    client, stub = stubbed("cloudwatch")
    now = datetime(2026, 10, 28, 12, 0, tzinfo=UTC)
    # queries: orders x {BytesIn, MessagesIn} x brokers {1,2} -> q0..q3 ; idle -> q4..q7
    stub.add_response(
        "get_metric_data",
        {
            "MetricDataResults": [
                {"Id": "q0", "Values": [100.0, 300.0]},
                {"Id": "q1", "Values": [50.0]},
                {"Id": "q4", "Values": []},
            ],
            "NextToken": "more",
        },
    )
    stub.add_response(
        "get_metric_data",
        {"MetricDataResults": [{"Id": "q2", "Values": [2.0]}, {"Id": "q3", "Values": [3.0]}]},
    )
    result = Metrics(client).topic_throughput("orders-prod", ["orders", "idle"], [1, 2], minutes=15, now=now)
    assert result["orders"].bytes_in_per_sec == 250.0  # avg(100,300) + 50
    assert result["orders"].messages_in_per_sec == 5.0
    assert result["idle"].bytes_in_per_sec is None


def test_topic_throughput_serverless_dimensions_and_errors():
    client, stub = stubbed("cloudwatch")
    stub.add_response("get_metric_data", {"MetricDataResults": [{"Id": "q0", "Values": [7.0]}]})
    result = Metrics(client).topic_throughput("c", ["t"], None, minutes=5)
    assert result["t"].bytes_in_per_sec == 7.0
    stub.add_client_error("get_metric_data", "AccessDenied")
    with pytest.raises(AccessDeniedError, match="cloudwatch:GetMetricData"):
        Metrics(client).topic_throughput("c", ["t"], None)
