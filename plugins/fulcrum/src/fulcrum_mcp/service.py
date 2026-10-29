"""Fulcrum service: owns the AWS/Kafka clients and renders each tool's markdown answer."""

from __future__ import annotations

import json
import logging
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, TypeVar

import boto3
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from botocore.config import Config
from botocore.exceptions import ProfileNotFound

from . import __version__
from .auth import MskIamTokenProvider
from .cache import TTLCache
from .config import Settings
from .credentials import SessionCredentialProvider, build_session
from .diff import diff_schemas
from .errors import ConfigError, FulcrumError, InputValidationError, translate_aws_error
from .formatting import document, human_bytes, kv_table, now_utc, paginate, table
from .glue_registry import GlueRegistry
from .kafka_admin import KafkaAdmin
from .metrics import Metrics
from .models import ClusterInfo
from .msk_client import MskClient, endpoint_label
from .validation import (
    validate_cluster_arn,
    validate_group_id,
    validate_page,
    validate_prefix,
    validate_registry_name,
    validate_schema_name,
    validate_topic,
    validate_version,
)

log = logging.getLogger(__name__)
T = TypeVar("T")

PRODUCER_LIMITATION = (
    "> **Kafka has no API that lists producers.** Producers are not registered with the cluster the "
    "way consumer groups are, so a complete producer list (client ids, hosts) is not available. "
    "Below is the best available evidence: per-topic ingress from CloudWatch (`AWS/Kafka` "
    "`BytesInPerSec` / `MessagesInPerSec`)."
)
VALID_GROUP_STATES = {"STABLE", "EMPTY", "DEAD", "PREPARING_REBALANCING", "COMPLETING_REBALANCING", "UNKNOWN"}


@dataclass(frozen=True)
class KafkaTarget:
    """Which brokers to talk to. ``bootstrap`` is set when the user gave broker endpoints directly;
    otherwise it is looked up from ``arn`` with kafka:GetBootstrapBrokers. ``arn`` is optional in
    direct mode and only enables the MSK-API extras (cluster state, CloudWatch metrics)."""

    key: str
    region: str
    arn: str | None = None
    bootstrap: str | None = None


class FulcrumService:
    def __init__(
        self,
        settings: Settings,
        session_factory: Callable[..., Any] = boto3.Session,
        admin_factory: Callable[[str, MskIamTokenProvider, float], KafkaAdmin] | None = None,
        clock: Callable[[], str] = now_utc,
    ) -> None:
        self.settings = settings
        self._session_factory = session_factory
        self._admin_factory = admin_factory or KafkaAdmin
        self._clock = clock
        self.cache = TTLCache(settings.cache_ttl)
        self._lock = threading.Lock()
        self._sessions: dict[str, Any] = {}
        self._clients: dict[tuple[str, str], Any] = {}
        self._admins: dict[str, KafkaAdmin] = {}
        self._default_region: str | None = None

    # -- clients ----------------------------------------------------------------------------

    def _profile_region(self) -> str | None:
        """Region from the AWS profile/config files, used when no region is configured explicitly."""
        if self._default_region is None:
            try:
                session = self._session_factory(profile_name=self.settings.profile)
            except ProfileNotFound as exc:
                raise self._profile_error() from exc
            self._default_region = getattr(session, "region_name", None) or ""
        return self._default_region or None

    def _region(self, arn: str | None = None) -> str:
        if arn:
            return arn.split(":")[3]
        region = self.settings.region or self._profile_region()
        if not region:
            raise ConfigError(
                "No AWS region configured.",
                hint="Set AWS_REGION or the plugin's `aws_region` option, then restart Claude Code.",
            )
        return region

    def _session(self, region: str) -> Any:
        """Session for ``region``: the base credentials, or the configured IAM role assumed from them.
        Callers hold ``self._lock`` or tolerate a duplicate session being built."""
        session = self._sessions.get(region)
        if session is None:
            try:
                session = build_session(
                    self.settings.profile,
                    region,
                    self.settings.role_arn,
                    self.settings.role_external_id,
                    session_factory=self._session_factory,
                )
            except ProfileNotFound as exc:
                raise self._profile_error() from exc
            self._sessions[region] = session
        return session

    def _profile_error(self) -> ConfigError:
        return ConfigError(
            f"AWS profile `{self.settings.profile}` was not found in your AWS config files.",
            hint="Check `aws configure list-profiles`, then fix AWS_PROFILE or the `aws_profile` option.",
        )

    def _client(self, service: str, region: str) -> Any:
        key = (service, region)
        with self._lock:
            if key not in self._clients:
                session = self._session(region)
                cfg = Config(
                    region_name=region,
                    connect_timeout=min(10, self.settings.timeout),
                    read_timeout=self.settings.timeout,
                    retries={"mode": "adaptive", "max_attempts": 6},
                    user_agent_extra=f"fulcrum-mcp/{__version__}",
                )
                self._clients[key] = session.client(service, config=cfg)
            return self._clients[key]

    def msk(self, region: str | None = None) -> MskClient:
        return MskClient(self._client("kafka", region or self._region()))

    def glue(self) -> GlueRegistry:
        return GlueRegistry(self._client("glue", self._region()), self.settings.registry_name)

    def metrics(self, region: str) -> Metrics:
        return Metrics(self._client("cloudwatch", region))

    def _cached(self, key: tuple[Any, ...], factory: Callable[[], T]) -> tuple[T, str]:
        """Return (value, fetched_at), reusing a cached value while it is fresh."""
        hit = self.cache.get(key)
        if hit is not None:
            return hit[0], f"{hit[1]} (cached)"
        value = factory()
        stamp = self._clock()
        self.cache.set(key, (value, stamp))
        return value, stamp

    def _doc(self, title: str, sections: list[str], stamp: str) -> str:
        return document(title, sections, clock=lambda: stamp)

    def resolve_cluster(self, override: str | None) -> str:
        if override:
            return validate_cluster_arn(override)
        if self.settings.cluster_arn:
            return self.settings.cluster_arn
        clusters = self.msk().list_clusters()
        if len(clusters) == 1:
            return clusters[0].arn
        if not clusters:
            raise ConfigError(
                f"No MSK clusters exist in {self._region()} for this account.",
                hint="Check the region (AWS_REGION / `aws_region`) or pass `cluster=<arn>` for a cluster elsewhere.",
            )
        listing = table(["Name", "Type", "State", "ARN"], [(c.name, c.cluster_type, c.state, c.arn) for c in clusters])
        raise ConfigError(
            f"No MSK cluster selected and {len(clusters)} clusters exist in {self._region()}.",
            hint=(
                f"{listing}\n\nPick one and either pass `cluster=<arn>` to the command, or set the plugin's "
                "`msk_cluster_arn` option (or the MSK_CLUSTER_ARN environment variable) and restart Claude Code."
            ),
        )

    def _target(self, override: str | None) -> KafkaTarget:
        """Pick the Kafka cluster for a command: an explicit ``cluster=<arn>``, else the configured
        bootstrap brokers, else the configured (or only) cluster ARN."""
        if override:
            arn = validate_cluster_arn(override)
            return KafkaTarget(key=arn, region=self._region(arn), arn=arn)
        if self.settings.bootstrap_brokers:
            arn = self.settings.cluster_arn
            region = self._region(arn) if arn else self._region()
            return KafkaTarget(
                key=f"brokers:{self.settings.bootstrap_brokers}",
                region=region,
                arn=arn,
                bootstrap=self.settings.bootstrap_brokers,
            )
        arn = self.resolve_cluster(None)
        return KafkaTarget(key=arn, region=self._region(arn), arn=arn)

    def admin(self, target: KafkaTarget) -> KafkaAdmin:
        with self._lock:
            existing = self._admins.get(target.key)
        if existing is not None:
            return existing
        bootstrap = target.bootstrap or self.msk(target.region).iam_bootstrap(target.arn or "")
        provider = self._token_provider(target.region)
        created = self._admin_factory(
            bootstrap, provider, self.settings.timeout, debug=self.settings.log_level == "DEBUG"
        )
        with self._lock:
            return self._admins.setdefault(target.key, created)

    def _token_provider(self, region: str) -> MskIamTokenProvider:
        """MSK IAM tokens signed with the same (possibly role-assumed) credentials as the AWS calls."""
        with self._lock:
            session = self._session(region)
        credentials = SessionCredentialProvider(session)
        return MskIamTokenProvider(
            region,
            self.settings.profile,
            token_fn=lambda: MSKAuthTokenProvider.generate_auth_token_from_credentials_provider(region, credentials),
        )

    def _cluster_info(self, arn: str) -> ClusterInfo:
        return self.msk(self._region(arn)).describe_cluster(arn)

    # -- status / clusters ------------------------------------------------------------------

    def status(self) -> str:
        s = self.settings
        rows: list[tuple[str, Any]] = [
            ("Fulcrum version", __version__),
            ("AWS region", s.region or "not set"),
            ("AWS profile", s.profile or "default credential chain"),
            ("IAM role", s.role_arn or "not set (use base credentials directly)"),
            ("Bootstrap brokers", s.bootstrap_brokers or "not set (looked up from the cluster ARN)"),
            (
                "MSK cluster ARN",
                s.cluster_arn
                or ("not set (optional with bootstrap brokers)" if s.bootstrap_brokers else "not set (auto-select)"),
            ),
            ("Glue registry", s.registry_name or "all registries"),
            ("Lag threshold", s.lag_threshold),
            ("Cache TTL (s)", s.cache_ttl),
            ("Request timeout (s)", s.timeout),
        ]
        checks: list[tuple[str, str, str]] = []
        try:
            region = self._region()
        except FulcrumError as err:
            checks.append(("AWS region", "failed", err.message))
            return self._doc(
                "Fulcrum status",
                [kv_table(rows), "### Checks", table(["Check", "Result", "Detail"], checks)],
                self._clock(),
            )
        if not s.region:
            rows[1] = ("AWS region", f"{region} (from AWS profile config)")
        identity_check = "IAM role assumed" if s.role_arn else "AWS credentials"
        try:
            ident = self._client("sts", region).get_caller_identity()
            checks.append((identity_check, "ok", ident.get("Arn", "")))
        except Exception as exc:
            err = translate_aws_error(exc, "sts:GetCallerIdentity")
            checks.append((identity_check, "failed", err.message))
        else:
            try:
                target = self._target(None)
                if target.bootstrap:
                    checks.append(("Kafka endpoint", "ok", f"{len(target.bootstrap.split(','))} bootstrap brokers"))
                else:
                    checks.append(("MSK cluster", "ok", target.arn or ""))
                broker_count = len(self.admin(target).broker_ids())
                checks.append(("Kafka brokers (IAM)", "ok", f"{broker_count} brokers reachable"))
            except FulcrumError as err:
                checks.append(("MSK / Kafka connectivity", "failed", err.message))
        return self._doc(
            "Fulcrum status",
            [kv_table(rows), "### Checks", table(["Check", "Result", "Detail"], checks)],
            self._clock(),
        )

    def list_clusters(self) -> str:
        clusters, stamp = self._cached(("clusters", self._region()), lambda: self.msk().list_clusters())
        rows = [(c.name, c.cluster_type, c.state, c.kafka_version, c.arn) for c in clusters]
        return self._doc(
            f"MSK clusters in {self._region()} ({len(clusters)})",
            [table(["Name", "Type", "State", "Kafka", "ARN"], rows)],
            stamp,
        )

    def cluster(self, cluster: str | None) -> str:
        target = self._target(cluster)
        if not target.arn:
            return self._cluster_from_brokers(target)
        arn = target.arn
        info, stamp = self._cached(("cluster", arn), lambda: self._cluster_info(arn))
        rows = [
            ("Name", info.name),
            ("ARN", info.arn),
            ("Type", info.cluster_type),
            ("State", info.state),
            ("Kafka version", info.kafka_version or ("managed" if info.cluster_type == "SERVERLESS" else None)),
            ("Broker count", info.broker_count),
            ("Broker type", info.broker_instance_type),
            ("Client auth", ", ".join(info.auth_types) or None),
            ("Encryption in transit", info.encryption_in_transit),
            ("Enhanced monitoring", info.enhanced_monitoring),
            ("Created", info.created),
        ]
        endpoints = table(
            ["Endpoint", "Bootstrap brokers"],
            sorted((endpoint_label(k), v) for k, v in info.endpoints.items()),
        )
        return self._doc(f"Cluster `{info.name}`", [kv_table(rows), "### Bootstrap endpoints", endpoints], stamp)

    def _cluster_from_brokers(self, target: KafkaTarget) -> str:
        """Cluster view straight from the brokers, for direct-endpoint mode without a cluster ARN."""
        info, stamp = self._cached(("cluster-kafka", target.key), lambda: self.admin(target).describe_cluster())
        rows = [
            ("Kafka cluster id", info["cluster_id"]),
            ("Region", target.region),
            ("Broker count", len(info["brokers"])),
            ("Controller broker", info["controller"]),
            ("Client auth", "SASL/IAM (configured bootstrap brokers)"),
            ("Bootstrap brokers", target.bootstrap),
        ]
        brokers = table(["Broker id", "Host", "Port", "Rack / AZ"], info["brokers"])
        note = (
            "_Kafka version, broker type, state and MSK endpoints come from the MSK API. Set the plugin's "
            "`msk_cluster_arn` option (needs `kafka:DescribeClusterV2`) to include them._"
        )
        return self._doc("Kafka cluster (via bootstrap brokers)", [kv_table(rows), "### Brokers", brokers, note], stamp)

    # -- topics -----------------------------------------------------------------------------

    def topics(
        self,
        prefix: str | None,
        include_internal: bool,
        limit: int | None,
        offset: int,
        cluster: str | None,
    ) -> str:
        prefix = validate_prefix(prefix)
        limit, offset = validate_page(limit or self.settings.max_rows, offset)
        target = self._target(cluster)
        all_topics, stamp = self._cached(("topics", target.key), lambda: self.admin(target).list_topics())
        internal_count = sum(1 for t in all_topics if t.internal)
        selected = [
            t for t in all_topics if (include_internal or not t.internal) and (not prefix or t.name.startswith(prefix))
        ]
        page, footer = paginate(selected, limit, offset)
        summary = f"**{len(selected):,} topics**" + (f" matching `{prefix}`" if prefix else "")
        if not include_internal and internal_count:
            summary += f" ({internal_count} internal hidden; add `internal=true` to show)"
        summary += f", {sum(t.partitions for t in selected):,} partitions total."
        rows = [(t.name, t.partitions, t.replication_factor, t.internal) for t in page]
        return self._doc(
            "Topics",
            [summary, table(["Topic", "Partitions", "Replication", "Internal"], rows), footer],
            stamp,
        )

    def topic(self, name: str, cluster: str | None) -> str:
        name = validate_topic(name)
        target = self._target(cluster)
        detail, stamp = self._cached(("topic", target.key, name), lambda: self.admin(target).describe_topic(name))
        parts = detail.partitions
        messages = [
            p.latest_offset - p.earliest_offset
            for p in parts
            if p.latest_offset is not None and p.earliest_offset is not None
        ]
        urp = [p for p in parts if p.under_replicated]
        offline = [p for p in parts if p.offline]
        summary = kv_table(
            [
                ("Partitions", len(parts)),
                ("Replication factor", max((len(p.replicas) for p in parts), default=0)),
                ("Internal", detail.internal),
                ("Messages retained (approx.)", sum(messages) if messages else None),
                ("Under-replicated partitions", len(urp)),
                ("Offline partitions", len(offline)),
                ("Size on disk", human_bytes(detail.size_bytes) or "n/a (not exposed by Kafka AdminClient)"),
            ]
        )
        prows = [
            (
                p.partition,
                p.leader if p.leader is not None else "OFFLINE",
                ",".join(map(str, p.replicas)),
                ",".join(map(str, p.isr)) + (" ⚠️" if p.under_replicated else ""),
                p.earliest_offset,
                p.latest_offset,
            )
            for p in parts
        ]
        partitions, footer = paginate(prows, self.settings.max_rows, 0)
        configs = table(["Config", "Value"], sorted(detail.configs.items()))
        return self._doc(
            f"Topic `{name}`",
            [
                summary,
                "### Partitions",
                table(["Partition", "Leader", "Replicas", "ISR", "Earliest", "Latest"], partitions),
                footer,
                "### Configuration (overrides and key settings)",
                configs,
            ],
            stamp,
        )

    # -- consumer groups --------------------------------------------------------------------

    def groups(self, prefix: str | None, state: str | None, limit: int | None, offset: int, cluster: str | None) -> str:
        prefix = validate_prefix(prefix)
        limit, offset = validate_page(limit or self.settings.max_rows, offset)
        wanted = state.strip().upper() if state else None
        if wanted and wanted not in VALID_GROUP_STATES:
            raise InputValidationError(f"state must be one of {', '.join(sorted(VALID_GROUP_STATES))}.")
        target = self._target(cluster)
        groups, stamp = self._cached(("groups", target.key), lambda: self.admin(target).list_groups())
        selected = [
            g for g in groups if (not prefix or g.group_id.startswith(prefix)) and (not wanted or g.state == wanted)
        ]
        by_state: dict[str, int] = {}
        for g in selected:
            by_state[g.state] = by_state.get(g.state, 0) + 1
        summary = f"**{len(selected):,} consumer groups**" + (
            " — " + ", ".join(f"{s.lower()}: {n}" for s, n in sorted(by_state.items())) if by_state else ""
        )
        page, footer = paginate(selected, limit, offset)
        rows = [(g.group_id, g.state, g.members, g.group_type) for g in page]
        return self._doc(
            "Consumer groups", [summary, table(["Group", "State", "Members", "Type"], rows), footer], stamp
        )

    def group(self, group_id: str, cluster: str | None) -> str:
        group_id = validate_group_id(group_id)
        target = self._target(cluster)
        detail, stamp = self._cached(
            ("group", target.key, group_id), lambda: self.admin(target).describe_group(group_id)
        )
        lagging = [p for p in detail.partitions if (p.lag or 0) > 0]
        summary = kv_table(
            [
                ("State", detail.state),
                ("Type", detail.group_type),
                ("Assignor", detail.partition_assignor),
                ("Coordinator broker", detail.coordinator),
                ("Members", len(detail.members)),
                ("Partitions with commits", len(detail.partitions)),
                ("Partitions lagging", len(lagging)),
                ("**Total lag**", detail.total_lag),
            ]
        )
        members = table(
            ["Client id", "Host", "Member id", "Assigned partitions"],
            [(m.client_id, m.host, m.member_id, _fmt_assignments(m.assignments)) for m in detail.members],
        )
        prow_all = sorted(detail.partitions, key=lambda p: (-(p.lag or 0), p.topic, p.partition))
        prows, footer = paginate(prow_all, self.settings.max_rows, 0)
        partitions = table(
            ["Topic", "Partition", "Committed", "End offset", "Lag", "Owner"],
            [(p.topic, p.partition, p.committed, p.end_offset, p.lag, p.owner) for p in prows],
        )
        if footer:
            footer = f"{footer} Sorted by lag, highest first."
        return self._doc(
            f"Consumer group `{group_id}`",
            [summary, "### Members", members, "### Partition lag", partitions, footer],
            stamp,
        )

    def consumers(self, group_prefix: str | None, limit: int | None, offset: int, cluster: str | None) -> str:
        group_prefix = validate_prefix(group_prefix)
        limit, offset = validate_page(limit or self.settings.max_rows, offset)
        target = self._target(cluster)
        clients, stamp = self._cached(("consumers", target.key), lambda: self.admin(target).list_consumers())
        selected = [c for c in clients if not group_prefix or c.group_id.startswith(group_prefix)]
        hosts = {c.host for c in selected}
        groups = {c.group_id for c in selected}
        summary = (
            f"**{len(selected):,} active consumer clients** across {len(groups):,} groups on {len(hosts):,} hosts."
        )
        page, footer = paginate(selected, limit, offset)
        rows = [(c.client_id, c.host, c.group_id, _fmt_assignments(c.assignments)) for c in page]
        return self._doc(
            "Active consumers",
            [summary, table(["Client id", "Host", "Group", "Assignments"], rows), footer],
            stamp,
        )

    # -- producers --------------------------------------------------------------------------

    def producers(self, topic_prefix: str | None, minutes: int, cluster: str | None) -> str:
        topic_prefix = validate_prefix(topic_prefix)
        if minutes < 1 or minutes > 1440:
            raise InputValidationError("minutes must be between 1 and 1440.")
        target = self._target(cluster)
        admin = self.admin(target)
        topics, _ = self._cached(("topics", target.key), admin.list_topics)
        names = [t.name for t in topics if not t.internal and (not topic_prefix or t.name.startswith(topic_prefix))]

        sections = [PRODUCER_LIMITATION]
        stamp = self._clock()
        throughput: dict[str, Any] = {}
        if target.arn is None:
            sections.append(
                "CloudWatch metrics need the cluster name, which comes from the MSK API. Set the plugin's "
                "`msk_cluster_arn` option alongside the bootstrap brokers to see per-topic ingress."
            )
        else:
            arn = target.arn
            try:
                info, _ = self._cached(("cluster", arn), lambda: self._cluster_info(arn))
                broker_ids = None if info.cluster_type == "SERVERLESS" else admin.broker_ids()
                throughput = self.metrics(target.region).topic_throughput(info.name, names, broker_ids, minutes)
            except FulcrumError as err:
                sections.append(f"CloudWatch metrics unavailable: {err.render()}")
        if throughput:
            active = sorted(
                (t for t in throughput.values() if (t.messages_in_per_sec or 0) > 0 or (t.bytes_in_per_sec or 0) > 0),
                key=lambda t: (-(t.bytes_in_per_sec or 0), t.topic),
            )
            idle = len(names) - len(active)
            sections.append(
                f"**{len(active):,} topics received writes** in the last {minutes} min; {idle:,} topics idle or "
                "without data."
            )
            page, footer = paginate(active, self.settings.max_rows, 0)
            sections.append(
                table(
                    ["Topic", "Messages in/s (avg)", "Bytes in/s (avg)"],
                    [(t.topic, t.messages_in_per_sec, human_bytes(t.bytes_in_per_sec)) for t in page],
                )
            )
            sections.append(footer)
        transactional = (
            "Transactional producer ids: not available — the installed confluent-kafka AdminClient does not "
            "expose ListTransactions/DescribeProducers."
        )
        if hasattr(admin.client, "list_transactions"):
            transactional = (
                "Transactional producer ids: the client library supports ListTransactions; "
                "Fulcrum does not query it yet."
            )
        sections.append(f"_{transactional}_")
        return self._doc("Producer activity (best available)", sections, stamp)

    # -- health -----------------------------------------------------------------------------

    def health(self, lag_threshold: int | None, cluster: str | None) -> str:
        threshold = self.settings.lag_threshold if lag_threshold is None else lag_threshold
        if threshold < 0:
            raise InputValidationError("lag_threshold must be >= 0.")
        target = self._target(cluster)
        info = self._cluster_info(target.arn) if target.arn else None
        admin = self.admin(target)
        partitions = admin.partition_health()
        lags = admin.group_lags()
        stamp = self._clock()

        urp = [(t, p) for t, p in partitions if p.under_replicated and not p.offline]
        offline = [(t, p) for t, p in partitions if p.offline]
        lagging = [g for g in lags if g.total_lag > threshold]
        state = info.state if info else None
        problems = (state not in (None, "ACTIVE")) + bool(urp) + bool(offline) + bool(lagging)
        verdict = "✅ **Healthy**" if problems == 0 else f"⚠️ **{problems} issue type(s) found**"

        summary = kv_table(
            [
                (
                    "Cluster state",
                    "n/a (no cluster ARN configured)" if state is None else state + ("" if state == "ACTIVE" else " ⚠️"),
                ),
                ("Partitions checked", len(partitions)),
                ("Under-replicated partitions", len(urp)),
                ("Offline partitions", len(offline)),
                ("Groups checked", len(lags)),
                (f"Groups with lag > {threshold:,}", len(lagging)),
            ]
        )
        sections = [verdict, summary]
        if offline:
            sections += [
                "### Offline partitions",
                table(["Topic", "Partition", "Replicas"], [(t, p.partition, _ids(p.replicas)) for t, p in offline]),
            ]
        if urp:
            rows, footer = paginate(urp, self.settings.max_rows, 0)
            sections += [
                "### Under-replicated partitions",
                table(
                    ["Topic", "Partition", "Leader", "Replicas", "ISR"],
                    [(t, p.partition, p.leader, _ids(p.replicas), _ids(p.isr)) for t, p in rows],
                ),
                footer,
            ]
        if lagging:
            rows, footer = paginate(lagging, self.settings.max_rows, 0)
            sections += [
                f"### Consumer groups with lag > {threshold:,}",
                table(
                    ["Group", "State", "Total lag", "Max partition lag", "Partitions lagging"],
                    [(g.group_id, g.state, g.total_lag, g.max_partition_lag, g.partitions_lagging) for g in rows],
                ),
                footer,
            ]
        title = f"Health of `{info.name}`" if info else "Health (via bootstrap brokers)"
        return self._doc(title, sections, stamp)

    # -- schemas ----------------------------------------------------------------------------

    def schemas(self, registry: str | None, prefix: str | None, limit: int | None, offset: int) -> str:
        registry = validate_registry_name(registry) if registry else None
        prefix = validate_prefix(prefix)
        limit, offset = validate_page(limit or self.settings.max_rows, offset)
        glue = self.glue()
        registries, stamp = self._cached(("registries", self._region()), glue.list_registries)
        known = [r.name for r in registries]
        schemas, _ = self._cached(
            ("schemas", self._region(), registry), lambda: glue.list_schemas(registry, known_registries=known)
        )
        selected = [s for s in schemas if not prefix or s.name.startswith(prefix)]
        if registry or glue.default_registry:
            shown = registry or glue.default_registry
            registries = [r for r in registries if r.name == shown]
        counts: dict[str, int] = {}
        for s in schemas:
            counts[s.registry] = counts.get(s.registry, 0) + 1
        reg_table = table(
            ["Registry", "Status", "Schemas", "Description"],
            [(r.name, r.status, counts.get(r.name, 0), r.description) for r in registries],
        )
        page, footer = paginate(selected, limit, offset)
        schema_table = table(
            ["Registry", "Schema", "Status", "Updated"], [(s.registry, s.name, s.status, s.updated) for s in page]
        )
        return self._doc(
            "Glue Schema Registry",
            [
                f"**{len(registries):,} registries**, **{len(selected):,} schemas**"
                + (f" matching `{prefix}`" if prefix else "")
                + ".",
                "### Registries",
                reg_table,
                "### Schemas",
                schema_table,
                footer,
                "Run `/fulcrum schema <name>` for format, compatibility and versions.",
            ],
            stamp,
        )

    def schema(self, name: str, version: int | str | None, registry: str | None) -> str:
        name = validate_schema_name(name)
        number = validate_version(version)
        registry = validate_registry_name(registry) if registry else None
        detail, stamp = self._cached(
            ("schema", self._region(), registry, name, number), lambda: self.glue().describe(name, registry, number)
        )
        s = detail.summary
        cur = detail.current
        if cur is None:  # pragma: no cover - describe() always sets it
            raise FulcrumError(f"Schema `{name}` has no versions.")
        meta = kv_table(
            [
                ("Registry", s.registry),
                ("Format", s.data_format),
                ("Compatibility", s.compatibility),
                ("Status", s.status),
                ("Latest version", s.latest_version),
                ("Showing version", cur.version),
                ("Version created", cur.created),
                ("Description", detail.description),
                ("ARN", detail.arn),
            ]
        )
        versions = table(
            ["Version", "Status", "Created"],
            [(v.version, v.status, v.created) for v in sorted(detail.versions, key=lambda v: -v.version)],
        )
        lang = {"AVRO": "json", "JSON": "json", "PROTOBUF": "protobuf"}.get(cur.data_format or "", "")
        definition = f"```{lang}\n{_pretty(cur.definition)}\n```"
        return self._doc(
            f"Schema `{name}` v{cur.version}",
            [meta, "### Definition", definition, f"### Versions ({len(detail.versions)})", versions],
            stamp,
        )

    def schema_diff(self, name: str, v1: int | str, v2: int | str, registry: str | None) -> str:
        name = validate_schema_name(name)
        a, b = validate_version(v1), validate_version(v2)
        if a is None or b is None:
            raise InputValidationError("Both versions must be explicit numbers, e.g. `schema-diff orders 3 4`.")
        registry = validate_registry_name(registry) if registry else None
        glue = self.glue()
        reg = glue.resolve_registry(name, registry)
        left = glue.get_version(name, reg, a)
        right = glue.get_version(name, reg, b)
        stamp = self._clock()
        result = diff_schemas(left.definition, right.definition, left.data_format or "", f"v{a}", f"v{b}")
        sections = [
            kv_table([("Registry", reg), ("Format", left.data_format), ("Compared", f"v{a} → v{b}")]),
        ]
        if not result.changed:
            sections.append("✅ The two versions are identical.")
        else:
            if result.structural:
                sections += [
                    "### Field changes",
                    table(["Change", "Field", "Detail"], result.structural),
                ]
            sections += ["### Unified diff", f"```diff\n{result.unified}\n```"]
        return self._doc(f"Schema diff `{name}` v{a} → v{b}", sections, stamp)


def _fmt_assignments(assignments: list[tuple[str, int]], max_items: int = 12) -> str:
    if not assignments:
        return "—"
    by_topic: dict[str, list[int]] = {}
    for topic, partition in assignments:
        by_topic.setdefault(topic, []).append(partition)
    parts = [f"{t}[{','.join(map(str, sorted(ps)))}]" for t, ps in sorted(by_topic.items())]
    text = "; ".join(parts[:max_items])
    if len(parts) > max_items:
        text += f"; +{len(parts) - max_items} more topics"
    return text


def _ids(values: list[int]) -> str:
    return ",".join(map(str, values))


def _pretty(definition: str) -> str:
    try:
        return json.dumps(json.loads(definition), indent=2)
    except (ValueError, TypeError):
        return definition.strip()
