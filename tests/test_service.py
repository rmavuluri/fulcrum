import json

import pytest

from fulcrum_mcp.errors import ConfigError, InputValidationError

from .conftest import BOOTSTRAP, CLOCK, CLUSTER_ARN, describe_cluster_response

ARN = {"ClusterArn": CLUSTER_ARN}


def expect_bootstrap(session, times=1):
    for _ in range(times):
        session.stub("kafka").add_response("get_bootstrap_brokers", BOOTSTRAP, ARN)


def test_topics_counts_hides_internal_and_caches(service_factory, session):
    svc = service_factory()
    expect_bootstrap(session)
    out = svc.topics(None, False, None, 0, None)
    assert out.startswith("## Topics")
    assert "**3 topics**" in out and "1 internal hidden" in out
    assert "__consumer_offsets" not in out
    assert f"_Fetched at {CLOCK}_" in out
    again = svc.topics(None, True, None, 0, None)
    assert "(cached)" in again and "__consumer_offsets" in again


def test_topics_prefix_and_paging(service_factory, session):
    svc = service_factory()
    expect_bootstrap(session)
    out = svc.topics("pay", False, None, 0, None)
    assert "**1 topics** matching `pay`" in out and "payments" in out and "orders" not in out
    paged = svc.topics(None, False, 1, 0, None)
    assert "Showing 1–1 of 3" in paged and "offset=1" in paged


def test_topic_detail(service_factory, session):
    svc = service_factory()
    expect_bootstrap(session)
    out = svc.topic("orders", None)
    assert "## Topic `orders`" in out
    assert "| Messages retained (approx.) | 150 |" in out
    assert "| Under-replicated partitions | 1 |" in out
    assert "2,3 ⚠️" in out
    assert "min.insync.replicas" in out
    with pytest.raises(InputValidationError):
        svc.topic("bad name", None)


def test_groups_group_and_consumers(service_factory, session):
    svc = service_factory()
    expect_bootstrap(session)
    out = svc.groups(None, None, None, 0, None)
    assert "**2 consumer groups** — empty: 1, stable: 1" in out
    stable = svc.groups(None, "stable", None, 0, None)
    assert "orders-consumer" in stable and "payments-audit" not in stable
    with pytest.raises(InputValidationError):
        svc.groups(None, "sleepy", None, 0, None)

    detail = svc.group("orders-consumer", None)
    assert "| **Total lag** | 10 |" in detail
    assert "orders[0,1]" in detail and "orders-service" in detail and "/10.0.1.5" not in detail

    consumers = svc.consumers(None, None, 0, None)
    assert "**1 active consumer clients** across 1 groups on 1 hosts." in consumers
    assert "_No rows._" in svc.consumers("zzz", None, 0, None)


def test_cluster_and_list_clusters(service_factory, session):
    svc = service_factory()
    kafka = session.stub("kafka")
    kafka.add_response("describe_cluster_v2", describe_cluster_response(), ARN)
    kafka.add_response("get_bootstrap_brokers", BOOTSTRAP, ARN)
    out = svc.cluster(None)
    assert "## Cluster `orders-prod`" in out
    assert "| Kafka version | 3.6.0 |" in out and "| Broker count | 3 |" in out
    assert "SASL/IAM (private)" in out

    kafka.add_response(
        "list_clusters_v2",
        {
            "ClusterInfoList": [
                {
                    "ClusterArn": CLUSTER_ARN,
                    "ClusterName": "orders-prod",
                    "ClusterType": "PROVISIONED",
                    "State": "ACTIVE",
                }
            ]
        },
        {},
    )
    assert "MSK clusters in us-east-1 (1)" in svc.list_clusters()


def test_cluster_resolution(service_factory, session):
    svc = service_factory(cluster_arn=None)
    kafka = session.stub("kafka")
    one = {"ClusterArn": CLUSTER_ARN, "ClusterName": "only", "ClusterType": "PROVISIONED", "State": "ACTIVE"}
    two = {**one, "ClusterArn": CLUSTER_ARN.replace("orders-prod", "other"), "ClusterName": "other"}
    kafka.add_response("list_clusters_v2", {"ClusterInfoList": [one]}, {})
    assert svc.resolve_cluster(None) == CLUSTER_ARN
    kafka.add_response("list_clusters_v2", {"ClusterInfoList": [one, two]}, {})
    with pytest.raises(ConfigError, match="2 clusters exist") as info:
        svc.resolve_cluster(None)
    assert "only" in info.value.render() and "other" in info.value.render()
    kafka.add_response("list_clusters_v2", {"ClusterInfoList": []}, {})
    with pytest.raises(ConfigError, match="No MSK clusters exist in us-east-1"):
        svc.resolve_cluster(None)
    assert svc.resolve_cluster(CLUSTER_ARN) == CLUSTER_ARN
    with pytest.raises(InputValidationError):
        svc.resolve_cluster("garbage")


def test_missing_region(service_factory):
    svc = service_factory(region=None)
    with pytest.raises(ConfigError, match="region"):
        svc.list_clusters()


def test_producers_reports_limitation_and_throughput(service_factory, session):
    svc = service_factory()
    kafka = session.stub("kafka")
    expect_bootstrap(session)  # connect to the brokers
    kafka.add_response("describe_cluster_v2", describe_cluster_response(), ARN)
    expect_bootstrap(session)  # describe_cluster also lists endpoints
    session.stub("cloudwatch").add_response(
        "get_metric_data", {"MetricDataResults": [{"Id": "q0", "Values": [2048.0]}, {"Id": "q3", "Values": [4.0]}]}
    )
    out = svc.producers(None, 15, None)
    assert "Kafka has no API that lists producers" in out
    assert "**1 topics received writes**" in out
    assert "2.0 KiB" in out
    assert "Transactional producer ids: not available" in out
    with pytest.raises(InputValidationError):
        svc.producers(None, 0, None)


def test_producers_when_cloudwatch_denied(service_factory, session):
    svc = service_factory()
    expect_bootstrap(session)
    session.stub("kafka").add_response("describe_cluster_v2", describe_cluster_response(serverless=True), ARN)
    expect_bootstrap(session)
    session.stub("cloudwatch").add_client_error("get_metric_data", "AccessDenied")
    out = svc.producers("orders", 15, None)
    assert "CloudWatch metrics unavailable" in out and "cloudwatch:GetMetricData" in out


def test_health(service_factory, session):
    svc = service_factory(lag_threshold=100)
    session.stub("kafka").add_response("describe_cluster_v2", describe_cluster_response(), ARN)
    expect_bootstrap(session, 2)
    out = svc.health(None, None)
    assert "⚠️ **3 issue type(s) found**" in out
    assert "### Offline partitions" in out and "broken" in out
    assert "### Under-replicated partitions" in out
    assert "### Consumer groups with lag > 100" in out and "payments-audit" in out
    assert "orders-consumer" not in out.split("### Consumer groups")[1]  # lag 10 < 100
    with pytest.raises(InputValidationError):
        svc.health(-1, None)


def test_health_all_good(service_factory, session, fake_admin_client):
    fake_admin_client.topics = {"orders": [(0, 1, [1, 2], [1, 2])]}
    fake_admin_client.committed = {}
    svc = service_factory()
    session.stub("kafka").add_response("describe_cluster_v2", describe_cluster_response(), ARN)
    expect_bootstrap(session, 2)
    assert "✅ **Healthy**" in svc.health(None, None)


SID = {"RegistryName": "prod", "SchemaName": "orders"}
V1 = {"type": "record", "name": "O", "fields": [{"name": "id", "type": "string"}]}
V2 = {"type": "record", "name": "O", "fields": [{"name": "id", "type": "string"}, {"name": "n", "type": "int"}]}


def test_schemas_listing(service_factory, session):
    svc = service_factory()
    glue = session.stub("glue")
    glue.add_response("list_registries", {"Registries": [{"RegistryName": "prod", "Status": "AVAILABLE"}]}, {})
    glue.add_response(
        "list_schemas",
        {"Schemas": [{"RegistryName": "prod", "SchemaName": "orders", "SchemaStatus": "AVAILABLE"}]},
        {"RegistryId": {"RegistryName": "prod"}},
    )
    out = svc.schemas(None, None, None, 0)
    assert "**1 registries**, **1 schemas**" in out and "| prod | AVAILABLE | 1 |" in out


def test_schemas_scoped_to_registry(service_factory, session):
    svc = service_factory()
    glue = session.stub("glue")
    glue.add_response("list_registries", {"Registries": [{"RegistryName": "prod"}, {"RegistryName": "dev"}]}, {})
    glue.add_response("list_schemas", {"Schemas": []}, {"RegistryId": {"RegistryName": "dev"}})
    out = svc.schemas("dev", "x", None, 0)
    assert "**1 registries**, **0 schemas** matching `x`" in out


def _version(number, body):
    return {"VersionNumber": number, "DataFormat": "AVRO", "SchemaDefinition": json.dumps(body), "Status": "AVAILABLE"}


def test_schema_detail(service_factory, session):
    svc = service_factory(registry_name="prod")
    glue = session.stub("glue")
    glue.add_response(
        "get_schema",
        {"SchemaName": "orders", "DataFormat": "AVRO", "Compatibility": "BACKWARD", "LatestSchemaVersion": 2},
        {"SchemaId": SID},
    )
    glue.add_response(
        "list_schema_versions", {"Schemas": [{"VersionNumber": 1}, {"VersionNumber": 2}]}, {"SchemaId": SID}
    )
    glue.add_response(
        "get_schema_version", _version(1, V1), {"SchemaId": SID, "SchemaVersionNumber": {"VersionNumber": 1}}
    )
    out = svc.schema("orders", "1", None)
    assert "## Schema `orders` v1" in out and "| Compatibility | BACKWARD |" in out
    assert "```json" in out and '"id"' in out
    assert "### Versions (2)" in out


def test_schema_diff(service_factory, session):
    svc = service_factory()
    glue = session.stub("glue")
    glue.add_response(
        "get_schema_version", _version(1, V1), {"SchemaId": SID, "SchemaVersionNumber": {"VersionNumber": 1}}
    )
    glue.add_response(
        "get_schema_version", _version(2, V2), {"SchemaId": SID, "SchemaVersionNumber": {"VersionNumber": 2}}
    )
    out = svc.schema_diff("orders", 1, 2, "prod")
    assert "### Field changes" in out and "| added | n |" in out and "```diff" in out

    glue.add_response(
        "get_schema_version", _version(1, V1), {"SchemaId": SID, "SchemaVersionNumber": {"VersionNumber": 1}}
    )
    glue.add_response(
        "get_schema_version", _version(1, V1), {"SchemaId": SID, "SchemaVersionNumber": {"VersionNumber": 1}}
    )
    assert "identical" in svc.schema_diff("orders", 1, 1, "prod")
    with pytest.raises(InputValidationError):
        svc.schema_diff("orders", "latest", 2, "prod")


def test_status_ok_and_failure(service_factory, session):
    svc = service_factory()
    session.stub("sts").add_response(
        "get_caller_identity", {"Arn": "arn:aws:iam::123456789012:user/dev", "Account": "123456789012", "UserId": "x"}
    )
    expect_bootstrap(session)
    out = svc.status()
    assert "| AWS credentials | ok | arn:aws:iam::123456789012:user/dev |" in out
    assert "3 brokers reachable" in out

    svc2 = service_factory()
    session.stub("sts").add_client_error("get_caller_identity", "ExpiredTokenException", "expired")
    assert "| AWS credentials | failed |" in svc2.status()


def test_status_connectivity_failure(service_factory, session, fake_admin_client):
    from confluent_kafka import KafkaError

    from .conftest import kafka_exc

    fake_admin_client.metadata_error = kafka_exc(KafkaError._TRANSPORT)
    svc = service_factory()
    session.stub("sts").add_response(
        "get_caller_identity", {"Arn": "arn:aws:iam::123456789012:user/dev", "Account": "123456789012", "UserId": "u"}
    )
    expect_bootstrap(session)
    assert "| MSK / Kafka connectivity | failed |" in svc.status()


def test_region_falls_back_to_profile_config(session, kafka_admin):
    from fulcrum_mcp.service import FulcrumService

    from .conftest import make_settings

    session.region_name = "ap-south-1"
    svc = FulcrumService(
        make_settings(region=None, profile="saml"),
        session_factory=lambda **_kw: session,
        admin_factory=lambda *a, **k: kafka_admin,
        clock=lambda: CLOCK,
    )
    session.stub("sts").add_response(
        "get_caller_identity", {"Arn": "arn:aws:iam::123456789012:user/dev", "Account": "123456789012", "UserId": "u"}
    )
    expect_bootstrap(session)
    out = svc.status()
    assert "ap-south-1 (from AWS profile config)" in out


def test_unknown_profile_and_missing_region_in_status(kafka_admin):
    from botocore.exceptions import ProfileNotFound

    from fulcrum_mcp.service import FulcrumService

    from .conftest import make_settings

    def no_profile(**_kw):
        raise ProfileNotFound(profile="ghost")

    svc = FulcrumService(make_settings(region=None, profile="ghost"), session_factory=no_profile)
    with pytest.raises(ConfigError, match="ghost"):
        svc.list_clusters()
    assert "| AWS region | failed |" in svc.status()
