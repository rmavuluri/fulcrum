"""Optional live test. Runs only with FULCRUM_INTEGRATION=1 plus real AWS credentials,
AWS_REGION and (optionally) MSK_CLUSTER_ARN, from a machine with network access to the brokers."""

import os

import pytest

pytestmark = pytest.mark.integration

if os.environ.get("FULCRUM_INTEGRATION") != "1":
    pytest.skip("set FULCRUM_INTEGRATION=1 to run live AWS tests", allow_module_level=True)


@pytest.fixture(autouse=True)
def _aws_env():
    """Override the conftest fixture that blanks AWS variables."""


@pytest.fixture
def svc():
    from fulcrum_mcp.config import load_settings
    from fulcrum_mcp.service import FulcrumService

    return FulcrumService(load_settings())


def test_live_status(svc):
    assert "| AWS credentials | ok |" in svc.status()


def test_live_cluster_and_topics(svc):
    assert "## Cluster" in svc.cluster(None)
    assert "## Topics" in svc.topics(None, False, None, 0, None)
    assert "## Consumer groups" in svc.groups(None, None, None, 0, None)
    assert "## Health" in svc.health(None, None)


def test_live_schemas(svc):
    assert "## Glue Schema Registry" in svc.schemas(None, None, None, 0)
