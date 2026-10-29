"""Direct bootstrap-broker mode and IAM role assumption."""

import boto3
import pytest
from botocore.exceptions import ClientError, ProfileNotFound
from moto import mock_aws

from fulcrum_mcp.config import load_settings
from fulcrum_mcp.credentials import SessionCredentialProvider, assume_role_error, build_session
from fulcrum_mcp.errors import AccessDeniedError, ConfigError, CredentialsError, FulcrumError, InputValidationError
from fulcrum_mcp.service import FulcrumService
from fulcrum_mcp.validation import (
    region_from_bootstrap,
    validate_bootstrap_brokers,
    validate_external_id,
    validate_role_arn,
)

from .conftest import BOOTSTRAP, CLOCK, CLUSTER_ARN, describe_cluster_response, make_settings

BROKERS = (
    "b-1.orders.abc123.c2.kafka.us-west-2.amazonaws.com:9098,b-2.orders.abc123.c2.kafka.us-west-2.amazonaws.com:9098"
)
ROLE = "arn:aws:iam::123456789012:role/FulcrumReadOnly"


# -- validation & config --------------------------------------------------------------------


def test_bootstrap_validation_and_region():
    assert (
        validate_bootstrap_brokers(" a.example.com:9098 , b.example.com:9098 ")
        == "a.example.com:9098,b.example.com:9098"
    )
    for bad in ["", "nohostport", "host:abc", "host:70000", "bad_host!:9098", ",".join(["h:1"] * 31)]:
        with pytest.raises(InputValidationError):
            validate_bootstrap_brokers(bad)
    assert region_from_bootstrap(BROKERS) == "us-west-2"
    assert region_from_bootstrap("boot-x.c1.kafka-serverless.eu-central-1.amazonaws.com:9098") == "eu-central-1"
    assert region_from_bootstrap("localhost:9098") is None


def test_role_and_external_id_validation():
    assert validate_role_arn(ROLE) == ROLE
    assert validate_role_arn("arn:aws:iam::123456789012:role/path/to/Role").endswith("Role")
    for bad in ["", "arn:aws:iam::123:role/x", "arn:aws:iam::123456789012:user/bob"]:
        with pytest.raises(InputValidationError):
            validate_role_arn(bad)
    assert validate_external_id("ext-123") == "ext-123"
    with pytest.raises(InputValidationError):
        validate_external_id("x")


def test_settings_for_brokers_and_role():
    s = load_settings(
        {
            "FULCRUM_OPT_BOOTSTRAP_BROKERS": BROKERS,
            "FULCRUM_OPT_IAM_ROLE_ARN": ROLE,
            "FULCRUM_OPT_IAM_ROLE_EXTERNAL_ID": "ext-123",
        }
    )
    assert s.bootstrap_brokers == BROKERS and s.role_arn == ROLE and s.role_external_id == "ext-123"
    assert s.region == "us-west-2"  # inferred from the broker hostnames
    env = load_settings({"MSK_BOOTSTRAP_BROKERS": BROKERS, "FULCRUM_IAM_ROLE_ARN": ROLE, "AWS_REGION": "us-east-1"})
    assert env.region == "us-east-1" and env.role_arn == ROLE
    with pytest.raises(ConfigError, match="external ID"):
        load_settings({"FULCRUM_IAM_ROLE_EXTERNAL_ID": "ext-123"})


# -- role assumption ------------------------------------------------------------------------


@mock_aws
def test_build_session_assumes_role():
    session = build_session(None, "us-east-1", ROLE, "ext-123")
    ident = session.client("sts").get_caller_identity()
    assert ":assumed-role/FulcrumReadOnly/fulcrum-mcp" in ident["Arn"]
    creds = SessionCredentialProvider(session).load()
    assert creds.access_key and creds.secret_key and creds.token


@mock_aws
def test_build_session_without_role_is_base_session():
    session = build_session(None, "us-east-1")
    assert isinstance(session, boto3.Session)


def test_build_session_without_base_credentials():
    class NoCreds:
        def get_credentials(self):
            return None

    with pytest.raises(CredentialsError):
        build_session(None, "us-east-1", ROLE, session_factory=lambda **_kw: NoCreds())
    with pytest.raises(Exception):
        SessionCredentialProvider(NoCreds()).load()


def test_assume_role_denied_is_explained(monkeypatch):
    from botocore.credentials import AssumeRoleCredentialFetcher

    def denied(self):
        raise ClientError({"Error": {"Code": "AccessDenied", "Message": "not authorized"}}, "AssumeRole")

    monkeypatch.setattr(AssumeRoleCredentialFetcher, "fetch_credentials", denied)
    session = build_session(None, "us-east-1", ROLE)
    with pytest.raises(AccessDeniedError) as info:
        session.get_credentials().get_frozen_credentials()
    assert info.value.action == "sts:AssumeRole"
    assert "trust policy" in info.value.render()


def test_assume_role_error_other_cases():
    original = FulcrumError("x")
    assert assume_role_error(original, ROLE) is original
    err = assume_role_error(ClientError({"Error": {"Code": "Throttling", "Message": "slow"}}, "AssumeRole"), ROLE)
    assert "sts:AssumeRole" in err.message


# -- service in direct-broker mode ----------------------------------------------------------


@pytest.fixture
def direct(session, kafka_admin):
    created = []

    def factory(bootstrap, provider, timeout, debug=False):
        created.append((bootstrap, provider))
        return kafka_admin

    svc = FulcrumService(
        make_settings(region="us-west-2", cluster_arn=None, bootstrap_brokers=BROKERS),
        session_factory=lambda **_kw: session,
        admin_factory=factory,
        clock=lambda: CLOCK,
    )
    return svc, created


def test_direct_mode_needs_no_msk_api(direct):
    svc, created = direct
    # No kafka (MSK API) stubs are registered: any MSK call would fail the test.
    assert "**3 topics**" in svc.topics(None, False, None, 0, None)
    assert "orders-consumer" in svc.groups(None, None, None, 0, None)
    assert "| **Total lag** | 10 |" in svc.group("orders-consumer", None)
    assert created[0][0] == BROKERS
    assert len(created) == 1  # one long-lived admin client


def test_direct_mode_cluster_health_producers(direct):
    svc, _ = direct
    cluster = svc.cluster(None)
    assert "## Kafka cluster (via bootstrap brokers)" in cluster and "| Broker count | 3 |" in cluster
    assert "msk_cluster_arn" in cluster
    health = svc.health(None, None)
    assert "## Health (via bootstrap brokers)" in health and "n/a (no cluster ARN configured)" in health
    assert "⚠️ **3 issue type(s) found**" in health  # offline, under-replicated, lag; unknown state not counted
    producers = svc.producers(None, 15, None)
    assert "Kafka has no API that lists producers" in producers and "msk_cluster_arn" in producers


def test_direct_mode_with_arn_uses_msk_extras(session, kafka_admin):
    svc = FulcrumService(
        make_settings(region="us-east-1", bootstrap_brokers=BROKERS),
        session_factory=lambda **_kw: session,
        admin_factory=lambda *a, **k: kafka_admin,
        clock=lambda: CLOCK,
    )
    kafka = session.stub("kafka")
    kafka.add_response("describe_cluster_v2", describe_cluster_response(), {"ClusterArn": CLUSTER_ARN})
    kafka.add_response("get_bootstrap_brokers", BOOTSTRAP, {"ClusterArn": CLUSTER_ARN})
    assert "## Health of `orders-prod`" in svc.health(None, None)


def test_cluster_override_still_wins(session, kafka_admin):
    seen = []
    svc = FulcrumService(
        make_settings(bootstrap_brokers=BROKERS),
        session_factory=lambda **_kw: session,
        admin_factory=lambda bootstrap, cb, t, debug=False: seen.append(bootstrap) or kafka_admin,
        clock=lambda: CLOCK,
    )
    session.stub("kafka").add_response("get_bootstrap_brokers", BOOTSTRAP, {"ClusterArn": CLUSTER_ARN})
    svc.topics(None, False, None, 0, CLUSTER_ARN)
    assert seen == [BOOTSTRAP["BootstrapBrokerStringSaslIam"]]


def test_direct_mode_status(direct, session):
    svc, _ = direct
    session.stub("sts").add_response(
        "get_caller_identity", {"Arn": "arn:aws:iam::123456789012:user/dev", "Account": "123456789012", "UserId": "u"}
    )
    out = svc.status()
    assert "| Kafka endpoint | ok | 2 bootstrap brokers |" in out
    assert "3 brokers reachable" in out
    assert "not set (optional with bootstrap brokers)" in out


def test_token_provider_signs_with_session_credentials(direct, monkeypatch):
    from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

    svc, created = direct
    seen = {}

    def fake(region, provider):
        seen["region"] = region
        seen["provider"] = provider
        return "signed-token", 1_700_000_000_000

    monkeypatch.setattr(MSKAuthTokenProvider, "generate_auth_token_from_credentials_provider", staticmethod(fake))
    svc.topics(None, False, None, 0, None)
    token, expiry = created[0][1]("cfg")
    assert token == "signed-token" and expiry == 1_700_000_000.0
    assert seen["region"] == "us-west-2" and isinstance(seen["provider"], SessionCredentialProvider)


@mock_aws
def test_service_end_to_end_with_role(kafka_admin):
    svc = FulcrumService(
        make_settings(region="us-east-1", role_arn=ROLE, bootstrap_brokers=BROKERS),
        admin_factory=lambda *a, **k: kafka_admin,
        clock=lambda: CLOCK,
    )
    out = svc.status()
    assert "| IAM role assumed | ok |" in out and "assumed-role/FulcrumReadOnly" in out


def test_profile_not_found_when_building_session(kafka_admin):
    def missing(**_kw):
        raise ProfileNotFound(profile="ghost")

    svc = FulcrumService(
        make_settings(profile="ghost"), session_factory=missing, admin_factory=lambda *a, **k: kafka_admin
    )
    with pytest.raises(ConfigError, match="ghost"):
        svc.list_clusters()


def test_service_builds_real_kafka_admin(session):
    """Uses the real KafkaAdmin class (not a test factory) so constructor wiring is checked."""
    from confluent_kafka.admin import AdminClient

    from fulcrum_mcp.kafka_admin import KafkaAdmin

    svc = FulcrumService(
        make_settings(region="us-west-2", cluster_arn=None, bootstrap_brokers=BROKERS, log_level="DEBUG"),
        session_factory=lambda **_kw: session,
    )
    admin = svc.admin(svc._target(None))
    assert isinstance(admin, KafkaAdmin)
    assert admin._factory is AdminClient
    assert admin._config["bootstrap.servers"] == BROKERS
    assert admin._config["log_level"] == 7
    assert admin.timeout == 2.0
