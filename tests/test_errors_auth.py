import concurrent.futures

import pytest
from botocore.exceptions import (
    ClientError,
    EndpointConnectionError,
    NoCredentialsError,
    NoRegionError,
    SSOTokenLoadError,
)
from confluent_kafka import KafkaError

from fulcrum_mcp.auth import MskIamTokenProvider
from fulcrum_mcp.errors import (
    AccessDeniedError,
    ConfigError,
    CredentialsError,
    FulcrumError,
    NetworkError,
    NotFoundError,
    translate_aws_error,
    translate_kafka_error,
)

from .conftest import kafka_exc


def client_error(code: str, msg: str = "nope") -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": msg}}, "Op")


def test_access_denied_names_action():
    err = translate_aws_error(client_error("AccessDeniedException"), "kafka:DescribeClusterV2")
    assert isinstance(err, AccessDeniedError)
    assert "kafka:DescribeClusterV2" in err.render()
    assert "fulcrum-readonly-policy.json" in err.render()


@pytest.mark.parametrize(
    ("exc", "kind"),
    [
        (NoCredentialsError(), CredentialsError),
        (NoRegionError(), ConfigError),
        (client_error("NotFoundException"), NotFoundError),
        (client_error("EntityNotFoundException"), NotFoundError),
        (client_error("ExpiredTokenException"), CredentialsError),
        (EndpointConnectionError(endpoint_url="https://kafka"), FulcrumError),
        (SSOTokenLoadError(error_msg="expired"), CredentialsError),
        (client_error("Weird"), FulcrumError),
        (RuntimeError("x"), FulcrumError),
    ],
)
def test_aws_translation(exc, kind):
    assert isinstance(translate_aws_error(exc, "glue:GetSchema", "r/s"), kind)


def test_fulcrum_error_passthrough():
    original = NotFoundError("gone")
    assert translate_aws_error(original, "x") is original
    assert translate_kafka_error(original, "x") is original


@pytest.mark.parametrize(
    ("exc", "kind", "action"),
    [
        (kafka_exc(KafkaError.TOPIC_AUTHORIZATION_FAILED), AccessDeniedError, "kafka-cluster:DescribeTopic"),
        (kafka_exc(KafkaError.GROUP_AUTHORIZATION_FAILED), AccessDeniedError, "kafka-cluster:DescribeGroup"),
        (kafka_exc(KafkaError.SASL_AUTHENTICATION_FAILED), AccessDeniedError, "kafka-cluster:Connect"),
        (kafka_exc(KafkaError._TRANSPORT), NetworkError, None),
        (kafka_exc(KafkaError._TIMED_OUT), NetworkError, None),
        (kafka_exc(KafkaError.UNKNOWN_TOPIC_OR_PART), NotFoundError, None),
        (kafka_exc(KafkaError.INVALID_REQUEST), FulcrumError, None),
        (concurrent.futures.TimeoutError(), NetworkError, None),
    ],
)
def test_kafka_translation(exc, kind, action):
    err = translate_kafka_error(exc, "op", "kafka-cluster:DescribeCluster")
    assert isinstance(err, kind)
    if action:
        assert err.action == action
    if kind is NetworkError:
        assert "VPN" in err.render()


def test_token_provider_converts_ms_to_seconds():
    provider = MskIamTokenProvider("us-east-1", token_fn=lambda: ("tok", 1_700_000_000_000))
    token, expiry = provider("ignored")
    assert token == "tok" and expiry == 1_700_000_000.0
    assert provider.last_error is None


def test_token_provider_records_error():
    def boom():
        raise NoCredentialsError()

    provider = MskIamTokenProvider("us-east-1", token_fn=boom)
    with pytest.raises(NoCredentialsError):
        provider(None)
    assert isinstance(provider.last_error, CredentialsError)


def test_token_provider_default_paths(monkeypatch):
    from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

    monkeypatch.setattr(MSKAuthTokenProvider, "generate_auth_token", staticmethod(lambda region: ("a", 1000)))
    monkeypatch.setattr(
        MSKAuthTokenProvider, "generate_auth_token_from_profile", staticmethod(lambda region, profile: ("b", 2000))
    )
    assert MskIamTokenProvider("us-east-1")()[0] == "a"
    assert MskIamTokenProvider("us-east-1", profile="ops")()[0] == "b"
