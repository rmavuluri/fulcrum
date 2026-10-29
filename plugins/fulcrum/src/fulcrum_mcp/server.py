"""MCP tool definitions. Every tool is read-only."""

from __future__ import annotations

import functools
import logging
import threading
import time
from collections.abc import Callable
from typing import Annotated, Any, TypeVar

from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import ToolAnnotations
from pydantic import Field

from . import __version__
from .config import load_settings
from .errors import FulcrumError
from .logging_setup import configure_logging
from .service import FulcrumService

log = logging.getLogger("fulcrum_mcp")
F = TypeVar("F", bound=Callable[..., str])

INSTRUCTIONS = (
    "Fulcrum gives read-only access to Amazon MSK (Kafka) clusters and the AWS Glue Schema Registry. "
    "Every tool returns ready-to-show markdown with counts and a fetched-at timestamp; present it as-is. "
    "Use `cluster_health` for 'is anything wrong / lagging' questions, `describe_consumer_group` for one "
    "group's lag, and `fulcrum_status` when a call fails with a configuration or connectivity error. "
    "No tool can create, change or delete anything."
)

mcp = MCPServer(name="fulcrum", version=__version__, instructions=INSTRUCTIONS)
READ_ONLY = ToolAnnotations(read_only_hint=True, destructive_hint=False, idempotent_hint=True, open_world_hint=True)

_service: FulcrumService | None = None
_service_lock = threading.Lock()


def get_service() -> FulcrumService:
    global _service
    with _service_lock:
        if _service is None:
            _service = FulcrumService(load_settings())
        return _service


def set_service(service: FulcrumService | None) -> None:
    """Testing hook."""
    global _service
    with _service_lock:
        _service = service


def tool(func: F) -> F:
    """Register ``func`` as a read-only tool that logs and returns user-facing errors."""

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> str:
        started = time.monotonic()
        outcome = "ok"
        try:
            return func(*args, **kwargs)
        except FulcrumError as err:
            outcome = type(err).__name__
            raise ToolError(err.render()) from err
        except Exception as exc:
            outcome = "unexpected"
            log.exception("tool_crashed", extra={"tool": func.__name__})
            raise ToolError(f"**Unexpected error** in `{func.__name__}`: {type(exc).__name__}: {exc}") from exc
        finally:
            log.info(
                "tool_call",
                extra={"tool": func.__name__, "outcome": outcome, "ms": int((time.monotonic() - started) * 1000)},
            )

    mcp.tool(annotations=READ_ONLY)(wrapper)
    return func


Cluster = Annotated[
    str | None,
    Field(description="MSK cluster ARN. Defaults to the configured cluster (or the only cluster in the region)."),
]
Prefix = Annotated[str | None, Field(description="Only include names starting with this prefix.")]
Limit = Annotated[int | None, Field(description="Max rows to return (default 100).", ge=1, le=1000)]
Offset = Annotated[int, Field(description="Rows to skip, for paging.", ge=0)]
Registry = Annotated[
    str | None, Field(description="Glue registry name. Defaults to the configured registry, else all registries.")
]


@tool
def fulcrum_status() -> str:
    """Show Fulcrum's resolved configuration and check AWS credentials, MSK access and broker connectivity.
    Use this to troubleshoot failures. Never shows secrets."""
    return get_service().status()


@tool
def list_clusters() -> str:
    """List the MSK clusters in the configured region (name, type, state, Kafka version, ARN)."""
    return get_service().list_clusters()


@tool
def get_cluster(cluster_arn: Cluster = None) -> str:
    """Describe an MSK cluster: ARN, Kafka version, broker count and type, auth types, state and
    bootstrap endpoints."""
    return get_service().cluster(cluster_arn)


@tool
def list_topics(
    prefix: Prefix = None,
    include_internal: Annotated[bool, Field(description="Include internal topics such as __consumer_offsets.")] = False,
    limit: Limit = None,
    offset: Offset = 0,
    cluster_arn: Cluster = None,
) -> str:
    """List Kafka topics with partition count, replication factor and internal flag, sorted by name."""
    return get_service().topics(prefix, include_internal, limit, offset, cluster_arn)


@tool
def describe_topic(
    name: Annotated[str, Field(description="Topic name.")],
    cluster_arn: Cluster = None,
) -> str:
    """Describe one topic: partitions with leader, replicas, ISR and offsets, approximate message count,
    under-replicated/offline partitions and non-default configs."""
    return get_service().topic(name, cluster_arn)


@tool
def list_consumer_groups(
    prefix: Prefix = None,
    state: Annotated[
        str | None, Field(description="Filter by state: STABLE, EMPTY, DEAD, PREPARING_REBALANCING, ...")
    ] = None,
    limit: Limit = None,
    offset: Offset = 0,
    cluster_arn: Cluster = None,
) -> str:
    """List consumer groups with state and member count."""
    return get_service().groups(prefix, state, limit, offset, cluster_arn)


@tool
def describe_consumer_group(
    group_id: Annotated[str, Field(description="Consumer group id.")],
    cluster_arn: Cluster = None,
) -> str:
    """Describe one consumer group: members, assigned partitions, committed offsets, end offsets,
    per-partition lag and total lag."""
    return get_service().group(group_id, cluster_arn)


@tool
def list_consumers(
    group_prefix: Annotated[str | None, Field(description="Only groups whose id starts with this prefix.")] = None,
    limit: Limit = None,
    offset: Offset = 0,
    cluster_arn: Cluster = None,
) -> str:
    """List active consumer clients across all groups: client id, host, group and assigned partitions."""
    return get_service().consumers(group_prefix, limit, offset, cluster_arn)


@tool
def get_producer_activity(
    topic_prefix: Annotated[str | None, Field(description="Only topics starting with this prefix.")] = None,
    minutes: Annotated[int, Field(description="Look-back window in minutes.", ge=1, le=1440)] = 15,
    cluster_arn: Cluster = None,
) -> str:
    """Best available producer information. Kafka has no API listing producers, so this returns per-topic
    ingress (CloudWatch BytesInPerSec/MessagesInPerSec) and states that limitation."""
    return get_service().producers(topic_prefix, minutes, cluster_arn)


@tool
def cluster_health(
    lag_threshold: Annotated[
        int | None, Field(description="Report groups whose total lag exceeds this (default from config).", ge=0)
    ] = None,
    cluster_arn: Cluster = None,
) -> str:
    """Health check: cluster state, under-replicated and offline partitions, and consumer groups whose lag
    exceeds the threshold (sorted by lag)."""
    return get_service().health(lag_threshold, cluster_arn)


@tool
def list_schemas(registry: Registry = None, prefix: Prefix = None, limit: Limit = None, offset: Offset = 0) -> str:
    """List Glue Schema Registry registries and their schemas."""
    return get_service().schemas(registry, prefix, limit, offset)


@tool
def get_schema(
    name: Annotated[str, Field(description="Schema name.")],
    version: Annotated[int | None, Field(description="Version number; latest when omitted.", ge=1)] = None,
    registry: Registry = None,
) -> str:
    """Show a Glue schema: definition, data format, compatibility mode and version history."""
    return get_service().schema(name, version, registry)


@tool
def diff_schema_versions(
    name: Annotated[str, Field(description="Schema name.")],
    v1: Annotated[int, Field(description="Older version number.", ge=1)],
    v2: Annotated[int, Field(description="Newer version number.", ge=1)],
    registry: Registry = None,
) -> str:
    """Diff two versions of a Glue schema: field-level changes plus a unified diff."""
    return get_service().schema_diff(name, v1, v2, registry)


def main() -> None:
    try:
        level = load_settings().log_level
    except FulcrumError:
        level = "INFO"  # surfaced to the user on the first tool call instead
    configure_logging(level)
    log.info("server_start", extra={"version": __version__})
    mcp.run("stdio")


if __name__ == "__main__":
    main()
