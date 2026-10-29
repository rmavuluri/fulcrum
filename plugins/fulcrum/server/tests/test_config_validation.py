import pytest

from fulcrum_mcp.config import load_settings
from fulcrum_mcp.errors import ConfigError, InputValidationError
from fulcrum_mcp.validation import (
    validate_cluster_arn,
    validate_group_id,
    validate_page,
    validate_prefix,
    validate_registry_name,
    validate_schema_name,
    validate_topic,
    validate_version,
)

from .conftest import CLUSTER_ARN


def test_defaults_from_empty_env():
    s = load_settings({})
    assert s.region is None and s.cluster_arn is None and s.registry_name is None
    assert s.lag_threshold == 1000 and s.cache_ttl == 30.0 and s.max_rows == 100


def test_user_config_beats_env_and_placeholders_are_ignored():
    env = {
        "FULCRUM_OPT_AWS_REGION": "eu-west-1",
        "AWS_REGION": "us-east-1",
        "FULCRUM_OPT_MSK_CLUSTER_ARN": "${user_config.msk_cluster_arn}",
        "MSK_CLUSTER_ARN": CLUSTER_ARN,
        "FULCRUM_OPT_GLUE_REGISTRY_NAME": "",
        "GLUE_REGISTRY_NAME": "prod-registry",
        "FULCRUM_OPT_LAG_THRESHOLD": "5000",
        "FULCRUM_CACHE_TTL": "0",
        "FULCRUM_OPT_AWS_PROFILE": "ops",
    }
    s = load_settings(env)
    assert s.region == "eu-west-1"
    assert s.cluster_arn == CLUSTER_ARN
    assert s.registry_name == "prod-registry"
    assert s.lag_threshold == 5000
    assert s.cache_ttl == 0
    assert s.profile == "ops"


def test_region_inferred_from_cluster_arn():
    assert load_settings({"MSK_CLUSTER_ARN": CLUSTER_ARN}).region == "us-east-1"


@pytest.mark.parametrize(
    "env",
    [{"FULCRUM_LAG_THRESHOLD": "lots"}, {"FULCRUM_LAG_THRESHOLD": "-1"}, {"FULCRUM_TIMEOUT": "0"}],
)
def test_bad_numbers(env):
    with pytest.raises(ConfigError):
        load_settings(env)


def test_bad_cluster_arn_in_env():
    with pytest.raises(InputValidationError):
        load_settings({"MSK_CLUSTER_ARN": "not-an-arn"})


@pytest.mark.parametrize("name", ["orders", "orders.v1", "a_b-c", "x" * 249])
def test_valid_topics(name):
    assert validate_topic(name) == name


@pytest.mark.parametrize("name", ["", "..", "has space", "semi;colon", "x" * 250, "$(rm)"])
def test_invalid_topics(name):
    with pytest.raises(InputValidationError):
        validate_topic(name)


def test_group_ids():
    assert validate_group_id(" my group ") == "my group"
    for bad in ["", "a\nb", "x" * 256]:
        with pytest.raises(InputValidationError):
            validate_group_id(bad)


def test_prefix():
    assert validate_prefix(None) is None
    assert validate_prefix("  ") is None
    assert validate_prefix("orders.") == "orders."
    with pytest.raises(InputValidationError):
        validate_prefix("bad prefix")


def test_glue_names():
    assert validate_schema_name("Orders-v1") == "Orders-v1"
    assert validate_registry_name("default-registry") == "default-registry"
    with pytest.raises(InputValidationError):
        validate_schema_name("bad/name")
    with pytest.raises(InputValidationError):
        validate_registry_name("")


def test_versions():
    assert validate_version(None) is None
    assert validate_version("latest") is None
    assert validate_version("v3") == 3
    assert validate_version(2) == 2
    for bad in ["zero", 0, "-1"]:
        with pytest.raises(InputValidationError):
            validate_version(bad)


def test_cluster_arn_and_page():
    assert validate_cluster_arn(CLUSTER_ARN) == CLUSTER_ARN
    with pytest.raises(InputValidationError):
        validate_cluster_arn("arn:aws:s3:::bucket")
    assert validate_page(10, 0) == (10, 0)
    with pytest.raises(InputValidationError):
        validate_page(0, 0)
    with pytest.raises(InputValidationError):
        validate_page(10, -1)
