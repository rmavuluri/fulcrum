import asyncio

import pytest
from mcp.server.mcpserver.exceptions import ToolError

from fulcrum_mcp import server
from fulcrum_mcp.errors import AccessDeniedError

from .conftest import BOOTSTRAP, CLUSTER_ARN

EXPECTED_TOOLS = {
    "fulcrum_status",
    "list_clusters",
    "get_cluster",
    "list_topics",
    "describe_topic",
    "list_consumer_groups",
    "describe_consumer_group",
    "list_consumers",
    "get_producer_activity",
    "cluster_health",
    "list_schemas",
    "get_schema",
    "diff_schema_versions",
}
READ_VERBS = ("list_", "get_", "describe_", "diff_", "cluster_health", "fulcrum_status")


def call(name, args=None):
    return asyncio.run(server.mcp.call_tool(name, args or {}))


def text(result):
    return result.content[0].text


@pytest.fixture(autouse=True)
def _reset():
    yield
    server.set_service(None)


def test_tool_inventory_is_read_only():
    tools = asyncio.run(server.mcp.list_tools())
    assert {t.name for t in tools} == EXPECTED_TOOLS
    for t in tools:
        assert t.annotations.read_only_hint is True and t.annotations.destructive_hint is False
        assert t.name.startswith(READ_VERBS), t.name


def test_tool_call_end_to_end(service_factory, session):
    server.set_service(service_factory())
    session.stub("kafka").add_response("get_bootstrap_brokers", BOOTSTRAP, {"ClusterArn": CLUSTER_ARN})
    out = text(call("list_topics", {"prefix": "ord"}))
    assert "orders" in out and "Fetched at" in out


def test_fulcrum_errors_become_tool_errors(service_factory, monkeypatch):
    svc = service_factory()
    server.set_service(svc)

    def denied(*_a, **_k):
        raise AccessDeniedError("kafka:ListClustersV2")

    monkeypatch.setattr(svc, "list_clusters", denied)
    with pytest.raises(ToolError, match="kafka:ListClustersV2"):
        call("list_clusters")


def test_unexpected_errors_are_reported(service_factory, monkeypatch):
    svc = service_factory()
    server.set_service(svc)
    monkeypatch.setattr(svc, "status", lambda: 1 / 0)
    with pytest.raises(ToolError, match="ZeroDivisionError"):
        call("fulcrum_status")


def test_argument_validation(service_factory):
    server.set_service(service_factory())
    with pytest.raises(ToolError):
        call("list_topics", {"limit": 0})


def test_get_service_loads_settings(monkeypatch):
    monkeypatch.setenv("AWS_REGION", "eu-central-1")
    server.set_service(None)
    assert server.get_service().settings.region == "eu-central-1"


def test_main_runs_stdio(monkeypatch):
    ran = []
    monkeypatch.setattr(server.mcp, "run", ran.append)
    monkeypatch.setenv("FULCRUM_LAG_THRESHOLD", "bad")  # config errors must not stop startup
    server.main()
    assert ran == ["stdio"]
