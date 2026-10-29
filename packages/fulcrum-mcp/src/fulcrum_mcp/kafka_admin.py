"""Read-only wrapper around one long-lived confluent-kafka AdminClient.

Only describe/list operations are ever called. The wrapper deliberately exposes no
method that creates, alters or deletes topics, groups, offsets or configs.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Hashable, Iterable
from concurrent.futures import Future
from typing import Any

from confluent_kafka import ConsumerGroupTopicPartitions, TopicCollection, TopicPartition
from confluent_kafka.admin import AdminClient, ConfigResource, ConfigSource, OffsetSpec, ResourceType

from .errors import FulcrumError, NotFoundError, translate_kafka_error
from .models import (
    ConsumerClient,
    GroupDetail,
    GroupLag,
    GroupMember,
    GroupSummary,
    PartitionInfo,
    PartitionLag,
    TopicDetail,
    TopicSummary,
)

log = logging.getLogger(__name__)

AdminFactory = Callable[[dict[str, Any]], Any]

# Topic configs worth showing even when they are still at their defaults.
KEY_TOPIC_CONFIGS = (
    "cleanup.policy",
    "retention.ms",
    "retention.bytes",
    "min.insync.replicas",
    "max.message.bytes",
    "compression.type",
)
_DESCRIBE_BATCH = 200
_DEFAULT_SOURCES = (ConfigSource.DEFAULT_CONFIG, ConfigSource.DEFAULT_CONFIG.value)


def _enum_name(value: Any) -> str:
    if value is None:
        return "UNKNOWN"
    return str(getattr(value, "name", value))


def is_internal_topic(name: str) -> bool:
    return name.startswith("__")


class KafkaAdmin:
    def __init__(
        self,
        bootstrap_servers: str,
        oauth_cb: Callable[[str | None], tuple[str, float]],
        timeout: float = 15.0,
        admin_factory: AdminFactory = AdminClient,
        debug: bool = False,
    ) -> None:
        self.timeout = timeout
        self._config: dict[str, Any] = {
            "bootstrap.servers": bootstrap_servers,
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "OAUTHBEARER",
            "oauth_cb": oauth_cb,
            "client.id": "fulcrum-mcp",
            "socket.timeout.ms": int(timeout * 1000),
            "socket.connection.setup.timeout.ms": int(timeout * 1000),
            "log.connection.close": False,
            # Unreachable brokers otherwise retry (and log) every ~100 ms for the life of the client.
            "reconnect.backoff.ms": 1000,
            "reconnect.backoff.max.ms": 30000,
            # librdkafka logs straight to stderr; Fulcrum reports failures itself, so keep it quiet
            # (critical only) unless FULCRUM_LOG_LEVEL=DEBUG.
            "log_level": 7 if debug else 2,
        }
        self._factory = admin_factory
        self._client: Any = None
        self._lock = threading.Lock()

    # -- plumbing ---------------------------------------------------------------------------

    @property
    def client(self) -> Any:
        with self._lock:
            if self._client is None:
                self._client = self._factory(self._config)
                # Serve the OAUTHBEARER callback once so the first request has a token.
                self._client.poll(1.0)
            return self._client

    @property
    def _request_timeout(self) -> float:
        return self.timeout

    def _result(self, future: Future, operation: str, iam_action: str | None = None) -> Any:
        try:
            return future.result(timeout=self.timeout + 5)
        except FulcrumError:
            raise
        except BaseException as exc:
            raise translate_kafka_error(exc, operation, iam_action) from exc

    def _results(
        self, futures: dict[Hashable, Future], operation: str, iam_action: str | None = None
    ) -> dict[Hashable, Any]:
        """Resolve a batch of futures; per-key failures are returned as FulcrumError values."""
        out: dict[Hashable, Any] = {}
        for key, future in futures.items():
            try:
                out[key] = self._result(future, operation, iam_action)
            except FulcrumError as err:
                out[key] = err
        return out

    def _metadata(self, topic: str | None = None) -> Any:
        try:
            return self.client.list_topics(topic=topic, timeout=self.timeout)
        except BaseException as exc:
            raise translate_kafka_error(exc, "fetching cluster metadata", "kafka-cluster:DescribeTopic") from exc

    # -- cluster ----------------------------------------------------------------------------

    def broker_ids(self) -> list[int]:
        return sorted(self._metadata().brokers.keys())

    def describe_cluster(self) -> dict[str, Any]:
        result = self._result(
            self.client.describe_cluster(request_timeout=self._request_timeout),
            "describing the cluster",
            "kafka-cluster:DescribeCluster",
        )
        return {
            "cluster_id": result.cluster_id,
            "controller": result.controller.id if result.controller else None,
            "brokers": sorted((n.id, n.host, n.port, n.rack) for n in result.nodes),
        }

    # -- topics -----------------------------------------------------------------------------

    def list_topics(self) -> list[TopicSummary]:
        md = self._metadata()
        topics = []
        for name, tmd in md.topics.items():
            partitions = list(tmd.partitions.values())
            rf = max((len(p.replicas) for p in partitions), default=0)
            topics.append(
                TopicSummary(
                    name=name, partitions=len(partitions), replication_factor=rf, internal=is_internal_topic(name)
                )
            )
        return sorted(topics, key=lambda t: t.name)

    def partition_health(self) -> list[tuple[str, PartitionInfo]]:
        """Every partition of every topic, from one metadata request."""
        md = self._metadata()
        out = []
        for name, tmd in md.topics.items():
            for p in tmd.partitions.values():
                leader = p.leader if p.leader is not None and p.leader >= 0 else None
                out.append(
                    (name, PartitionInfo(partition=p.id, leader=leader, replicas=list(p.replicas), isr=list(p.isrs)))
                )
        return sorted(out, key=lambda x: (x[0], x[1].partition))

    def describe_topic(self, name: str) -> TopicDetail:
        client = self.client
        desc_fs = client.describe_topics(TopicCollection([name]), request_timeout=self._request_timeout)
        resource = ConfigResource(ResourceType.TOPIC, name)
        cfg_fs = client.describe_configs([resource], request_timeout=self._request_timeout)

        desc = self._result(desc_fs[name], f"describing topic `{name}`", "kafka-cluster:DescribeTopic")
        partitions = [
            PartitionInfo(
                partition=p.id,
                leader=p.leader.id if p.leader is not None and p.leader.id >= 0 else None,
                replicas=[r.id for r in p.replicas],
                isr=[r.id for r in p.isr],
            )
            for p in sorted(desc.partitions, key=lambda p: p.id)
        ]

        tps = [TopicPartition(name, p.partition) for p in partitions]
        earliest = self._offsets(tps, OffsetSpec.earliest())
        latest = self._offsets(tps, OffsetSpec.latest())
        for p in partitions:
            p.earliest_offset = earliest.get((name, p.partition))
            p.latest_offset = latest.get((name, p.partition))

        configs: dict[str, str] = {}
        try:
            entries = self._result(
                cfg_fs[resource], f"describing configs of `{name}`", "kafka-cluster:DescribeTopicDynamicConfiguration"
            )
            for key, entry in sorted(entries.items()):
                is_default = getattr(entry, "is_default", False) or entry.source in _DEFAULT_SOURCES
                if entry.is_sensitive:
                    continue
                if not is_default or key in KEY_TOPIC_CONFIGS:
                    configs[key] = "" if entry.value is None else str(entry.value)
        except FulcrumError as err:
            configs["(unavailable)"] = err.message
        return TopicDetail(name=name, internal=desc.is_internal, partitions=partitions, configs=configs)

    def _offsets(self, tps: Iterable[TopicPartition], spec: OffsetSpec) -> dict[tuple[str, int], int]:
        request = dict.fromkeys(tps, spec)
        if not request:
            return {}
        futures = self.client.list_offsets(request, request_timeout=self._request_timeout)
        out: dict[tuple[str, int], int] = {}
        for tp, value in self._results(futures, "listing partition offsets", "kafka-cluster:DescribeTopic").items():
            if isinstance(value, FulcrumError):
                log.debug("offset_lookup_failed", extra={"topic": tp.topic, "partition": tp.partition})
                continue
            out[(tp.topic, tp.partition)] = value.offset
        return out

    # -- consumer groups --------------------------------------------------------------------

    def _group_listings(self) -> list[Any]:
        result = self._result(
            self.client.list_consumer_groups(request_timeout=self._request_timeout),
            "listing consumer groups",
            "kafka-cluster:DescribeGroup",
        )
        for err in result.errors or []:
            log.warning("list_groups_partial_error", extra={"error": str(err)})
        return list(result.valid or [])

    def _describe_groups(self, group_ids: list[str]) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for start in range(0, len(group_ids), _DESCRIBE_BATCH):
            batch = group_ids[start : start + _DESCRIBE_BATCH]
            futures = self.client.describe_consumer_groups(batch, request_timeout=self._request_timeout)
            out.update(self._results(futures, "describing consumer groups", "kafka-cluster:DescribeGroup"))
        return out

    def list_groups(self) -> list[GroupSummary]:
        listings = self._group_listings()
        described = self._describe_groups([g.group_id for g in listings])
        groups = []
        for listing in listings:
            desc = described.get(listing.group_id)
            if isinstance(desc, FulcrumError) or desc is None:
                state, members = _enum_name(listing.state), 0
            else:
                state, members = _enum_name(desc.state), len(desc.members)
            groups.append(
                GroupSummary(
                    group_id=listing.group_id,
                    state=state,
                    members=members,
                    group_type=_enum_name(getattr(listing, "type", None)),
                    simple=bool(listing.is_simple_consumer_group),
                )
            )
        return sorted(groups, key=lambda g: g.group_id)

    def list_consumers(self) -> list[ConsumerClient]:
        listings = self._group_listings()
        described = self._describe_groups([g.group_id for g in listings])
        clients = []
        for gid, desc in described.items():
            if isinstance(desc, FulcrumError):
                continue
            for m in desc.members:
                clients.append(
                    ConsumerClient(
                        group_id=str(gid),
                        member_id=m.member_id,
                        client_id=m.client_id,
                        host=m.host.lstrip("/"),
                        assignments=_assignments(m),
                    )
                )
        return sorted(clients, key=lambda c: (c.group_id, c.client_id, c.member_id))

    def _committed_offsets(self, group_ids: list[str]) -> dict[str, Any]:
        """Committed offsets per group. librdkafka allows one group per request, so all
        requests are issued up front and resolved together."""
        futures: dict[Hashable, Future] = {}
        for gid in group_ids:
            fs = self.client.list_consumer_group_offsets(
                [ConsumerGroupTopicPartitions(gid)], request_timeout=self._request_timeout
            )
            futures.update(fs)
        return self._results(futures, "listing committed offsets", "kafka-cluster:DescribeGroup")

    def describe_group(self, group_id: str) -> GroupDetail:
        desc = self._describe_groups([group_id]).get(group_id)
        if isinstance(desc, FulcrumError):
            raise desc
        committed = self._committed_offsets([group_id]).get(group_id)
        if isinstance(committed, FulcrumError):
            raise committed

        owners: dict[tuple[str, int], str] = {}
        members = []
        for m in desc.members:
            assigned = _assignments(m)
            for tp in assigned:
                owners[tp] = m.client_id or m.member_id
            members.append(
                GroupMember(
                    member_id=m.member_id,
                    client_id=m.client_id,
                    host=m.host.lstrip("/"),
                    group_instance_id=m.group_instance_id,
                    assignments=assigned,
                )
            )

        committed_map = {
            (tp.topic, tp.partition): tp.offset
            for tp in (committed.topic_partitions or [])
            if not getattr(tp, "error", None)
        }
        keys = sorted(set(committed_map) | set(owners))
        ends = self._offsets([TopicPartition(t, p) for t, p in keys], OffsetSpec.latest())
        partitions = [
            PartitionLag(
                topic=t,
                partition=p,
                committed=committed_map.get((t, p)) if committed_map.get((t, p), -1) >= 0 else None,
                end_offset=ends.get((t, p)),
                owner=owners.get((t, p)),
            )
            for t, p in keys
        ]
        if _enum_name(desc.state) == "DEAD" and not partitions:
            raise NotFoundError(f"Consumer group `{group_id}` does not exist.")
        return GroupDetail(
            group_id=group_id,
            state=_enum_name(desc.state),
            group_type=_enum_name(getattr(desc, "type", None)),
            partition_assignor=desc.partition_assignor or None,
            coordinator=desc.coordinator.id if desc.coordinator else None,
            members=sorted(members, key=lambda m: (m.client_id, m.member_id)),
            partitions=partitions,
        )

    def group_lags(self, group_ids: list[str] | None = None) -> list[GroupLag]:
        """Lag summary for many groups using one offsets request per group plus a single
        batched end-offset lookup."""
        listings = {g.group_id: g for g in self._group_listings()}
        ids = sorted(listings) if group_ids is None else group_ids
        committed = self._committed_offsets(ids)
        per_group: dict[str, dict[tuple[str, int], int]] = {}
        for gid, value in committed.items():
            if isinstance(value, FulcrumError):
                continue
            per_group[str(gid)] = {
                (tp.topic, tp.partition): tp.offset
                for tp in (value.topic_partitions or [])
                if tp.offset is not None and tp.offset >= 0 and not getattr(tp, "error", None)
            }
        all_tps = sorted({tp for offsets in per_group.values() for tp in offsets})
        ends = self._offsets([TopicPartition(t, p) for t, p in all_tps], OffsetSpec.latest())
        out = []
        for gid, offsets in per_group.items():
            lags = [max(ends[tp] - off, 0) for tp, off in offsets.items() if tp in ends]
            listing = listings.get(gid)
            out.append(
                GroupLag(
                    group_id=gid,
                    state=_enum_name(listing.state) if listing else "UNKNOWN",
                    total_lag=sum(lags),
                    max_partition_lag=max(lags, default=0),
                    partitions_lagging=sum(1 for lag in lags if lag > 0),
                )
            )
        return sorted(out, key=lambda g: (-g.total_lag, g.group_id))


def _assignments(member: Any) -> list[tuple[str, int]]:
    assignment = getattr(member, "assignment", None)
    tps = getattr(assignment, "topic_partitions", None) or []
    return sorted((tp.topic, tp.partition) for tp in tps)
