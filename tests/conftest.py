"""Shared fakes: an in-memory AdminClient built from real confluent-kafka model objects, and
boto3 sessions whose clients are wrapped in botocore Stubbers."""

from __future__ import annotations

import os
from concurrent.futures import Future
from types import SimpleNamespace
from typing import Any

import boto3
import pytest
from botocore.stub import Stubber
from confluent_kafka import (
    ConsumerGroupState,
    ConsumerGroupTopicPartitions,
    ConsumerGroupType,
    KafkaError,
    KafkaException,
    Node,
    TopicPartition,
    TopicPartitionInfo,
)
from confluent_kafka.admin import (
    ConfigEntry,
    ConfigSource,
    ConsumerGroupDescription,
    ConsumerGroupListing,
    DescribeClusterResult,
    ListConsumerGroupsResult,
    ListOffsetsResultInfo,
    MemberAssignment,
    MemberDescription,
    TopicDescription,
)

from fulcrum_mcp.config import Settings
from fulcrum_mcp.kafka_admin import KafkaAdmin
from fulcrum_mcp.service import FulcrumService

CLUSTER_ARN = "arn:aws:kafka:us-east-1:123456789012:cluster/orders-prod/11111111-2222-3333-4444-555555555555-1"
CLOCK = "2026-10-28 12:00:00 UTC"


@pytest.fixture(autouse=True)
def _aws_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make sure no test can reach a real AWS account."""
    for key in list(os.environ):
        if key.startswith(("AWS_", "MSK_", "GLUE_", "FULCRUM_")) and key != "FULCRUM_INTEGRATION":
            monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_CONFIG_FILE", "/dev/null")
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", "/dev/null")


def done(value: Any) -> Future:
    f: Future = Future()
    f.set_result(value)
    return f


def failed(exc: BaseException) -> Future:
    f: Future = Future()
    f.set_exception(exc)
    return f


def kafka_exc(code: int, reason: str = "boom") -> KafkaException:
    return KafkaException(KafkaError(code, reason))


def node(i: int) -> Node:
    return Node(i, f"b-{i}.orders.kafka.us-east-1.amazonaws.com", 9098)


class FakeAdminClient:
    """Implements just the read-only AdminClient surface Fulcrum uses."""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.polled = False
        # topic -> list of (partition, leader, replicas, isr)
        self.topics: dict[str, list[tuple[int, int, list[int], list[int]]]] = {
            "orders": [(0, 1, [1, 2, 3], [1, 2, 3]), (1, 2, [2, 3, 1], [2, 3])],
            "payments": [(0, 3, [3, 1, 2], [3, 1, 2])],
            "__consumer_offsets": [(0, 1, [1, 2, 3], [1, 2, 3])],
            "broken": [(0, -1, [1, 2], [])],
        }
        self.earliest = {("orders", 0): 10, ("orders", 1): 0, ("payments", 0): 0, ("broken", 0): 0}
        self.latest = {("orders", 0): 110, ("orders", 1): 50, ("payments", 0): 5000, ("broken", 0): 0}
        self.configs = {
            "retention.ms": ConfigEntry("retention.ms", "604800000", source=ConfigSource.DEFAULT_CONFIG),
            "cleanup.policy": ConfigEntry("cleanup.policy", "delete", source=ConfigSource.DEFAULT_CONFIG),
            "segment.bytes": ConfigEntry("segment.bytes", "1073741824", source=ConfigSource.DEFAULT_CONFIG),
            "min.insync.replicas": ConfigEntry("min.insync.replicas", "2", source=ConfigSource.DYNAMIC_TOPIC_CONFIG),
            "sasl.secret": ConfigEntry(
                "sasl.secret", "hidden", source=ConfigSource.DYNAMIC_TOPIC_CONFIG, is_sensitive=True
            ),
        }
        member = MemberDescription(
            "m-1",
            "orders-service",
            "/10.0.1.5",
            MemberAssignment([TopicPartition("orders", 0), TopicPartition("orders", 1)]),
        )
        self.groups = {
            "orders-consumer": ConsumerGroupDescription(
                "orders-consumer",
                False,
                [member],
                "range",
                ConsumerGroupState.STABLE,
                node(1),
                type=ConsumerGroupType.CLASSIC,
            ),
            "payments-audit": ConsumerGroupDescription(
                "payments-audit", False, [], "", ConsumerGroupState.EMPTY, node(2), type=ConsumerGroupType.CLASSIC
            ),
        }
        self.committed = {
            "orders-consumer": [TopicPartition("orders", 0, 100), TopicPartition("orders", 1, 50)],
            "payments-audit": [TopicPartition("payments", 0, 1000)],
        }
        self.metadata_error: BaseException | None = None
        self.describe_group_errors: dict[str, BaseException] = {}

    # metadata ---------------------------------------------------------------
    def poll(self, timeout: float) -> int:
        self.polled = True
        return 0

    def list_topics(self, topic: str | None = None, timeout: float = -1) -> Any:
        self.calls.append("list_topics")
        if self.metadata_error:
            raise self.metadata_error
        topics = {
            name: SimpleNamespace(
                topic=name,
                partitions={
                    pid: SimpleNamespace(id=pid, leader=leader, replicas=replicas, isrs=isr, error=None)
                    for pid, leader, replicas, isr in parts
                },
            )
            for name, parts in self.topics.items()
        }
        brokers = {i: SimpleNamespace(id=i, host=node(i).host, port=9098) for i in (1, 2, 3)}
        return SimpleNamespace(topics=topics, brokers=brokers, cluster_id="abc", controller_id=1)

    def describe_cluster(self, request_timeout: float = 0) -> Future:
        self.calls.append("describe_cluster")
        return done(DescribeClusterResult(node(1), [node(1), node(2), node(3)], cluster_id="abc"))

    def describe_topics(self, topics: Any, request_timeout: float = 0) -> dict[str, Future]:
        self.calls.append("describe_topics")
        out = {}
        for name in topics.topic_names:
            if name not in self.topics:
                out[name] = failed(kafka_exc(KafkaError.UNKNOWN_TOPIC_OR_PART, "Broker: Unknown topic"))
                continue
            parts = [
                TopicPartitionInfo(
                    pid,
                    node(leader) if leader >= 0 else Node(-1, "", -1),
                    [node(r) for r in rep],
                    [node(i) for i in isr],
                )
                for pid, leader, rep, isr in self.topics[name]
            ]
            out[name] = done(TopicDescription(name, None, name.startswith("__"), parts))
        return out

    def describe_configs(self, resources: list[Any], request_timeout: float = 0) -> dict[Any, Future]:
        self.calls.append("describe_configs")
        return {r: done(dict(self.configs)) for r in resources}

    def list_offsets(self, request: dict[TopicPartition, Any], request_timeout: float = 0) -> dict[Any, Future]:
        self.calls.append("list_offsets")
        out = {}
        for tp, spec in request.items():
            table = self.latest if spec.__class__.__name__.lower().startswith("latest") else self.earliest
            key = (tp.topic, tp.partition)
            out[tp] = (
                done(ListOffsetsResultInfo(table[key], -1, -1))
                if key in table
                else failed(kafka_exc(KafkaError.UNKNOWN_TOPIC_OR_PART))
            )
        return out

    # groups -----------------------------------------------------------------
    def list_consumer_groups(self, request_timeout: float = 0) -> Future:
        self.calls.append("list_consumer_groups")
        return done(
            ListConsumerGroupsResult(
                valid=[
                    ConsumerGroupListing(g.group_id, False, g.state, ConsumerGroupType.CLASSIC)
                    for g in self.groups.values()
                ]
            )
        )

    def describe_consumer_groups(self, group_ids: list[str], request_timeout: float = 0) -> dict[str, Future]:
        self.calls.append("describe_consumer_groups")
        out = {}
        for gid in group_ids:
            if gid in self.describe_group_errors:
                out[gid] = failed(self.describe_group_errors[gid])
            elif gid in self.groups:
                out[gid] = done(self.groups[gid])
            else:
                out[gid] = done(ConsumerGroupDescription(gid, False, [], "", ConsumerGroupState.DEAD, node(1)))
        return out

    def list_consumer_group_offsets(self, requests: list[Any], request_timeout: float = 0) -> dict[str, Future]:
        self.calls.append("list_consumer_group_offsets")
        assert len(requests) == 1, "librdkafka allows one group per request"
        gid = requests[0].group_id
        return {gid: done(ConsumerGroupTopicPartitions(gid, list(self.committed.get(gid, []))))}


@pytest.fixture
def fake_admin_client() -> FakeAdminClient:
    return FakeAdminClient()


@pytest.fixture
def kafka_admin(fake_admin_client: FakeAdminClient) -> KafkaAdmin:
    return KafkaAdmin("b-1:9098", lambda _cfg: ("token", 0.0), timeout=2, admin_factory=lambda _conf: fake_admin_client)


class FakeSession:
    """Stands in for boto3.Session; returns real clients with Stubbers attached."""

    def __init__(self, region_name: str | None = None) -> None:
        self.region_name = region_name
        self.clients: dict[str, Any] = {}
        self.stubbers: dict[str, Stubber] = {}

    def client(self, service: str, config: Any = None) -> Any:
        return self.get(service)

    def get(self, service: str) -> Any:
        if service not in self.clients:
            client = boto3.client(service, region_name="us-east-1")
            stubber = Stubber(client)
            stubber.activate()
            self.clients[service] = client
            self.stubbers[service] = stubber
        return self.clients[service]

    def stub(self, service: str) -> Stubber:
        self.get(service)
        return self.stubbers[service]


def make_settings(**overrides: Any) -> Settings:
    base: dict[str, Any] = {
        "region": "us-east-1",
        "profile": None,
        "cluster_arn": CLUSTER_ARN,
        "registry_name": None,
        "lag_threshold": 1000,
        "cache_ttl": 30.0,
        "timeout": 2.0,
        "max_rows": 100,
    }
    base.update(overrides)
    return Settings(**base)


@pytest.fixture
def session() -> FakeSession:
    return FakeSession()


@pytest.fixture
def service_factory(session: FakeSession, kafka_admin: KafkaAdmin):
    def build(**overrides: Any) -> FulcrumService:
        return FulcrumService(
            make_settings(**overrides),
            session_factory=lambda **_kw: session,
            admin_factory=lambda bootstrap, cb, timeout, debug=False: kafka_admin,
            clock=lambda: CLOCK,
        )

    return build


def describe_cluster_response(state: str = "ACTIVE", serverless: bool = False) -> dict[str, Any]:
    if serverless:
        return {
            "ClusterInfo": {
                "ClusterArn": CLUSTER_ARN,
                "ClusterName": "orders-prod",
                "ClusterType": "SERVERLESS",
                "State": state,
                "Serverless": {
                    "VpcConfigs": [{"SubnetIds": ["subnet-1"]}],
                    "ClientAuthentication": {"Sasl": {"Iam": {"Enabled": True}}},
                },
            }
        }
    return {
        "ClusterInfo": {
            "ClusterArn": CLUSTER_ARN,
            "ClusterName": "orders-prod",
            "ClusterType": "PROVISIONED",
            "State": state,
            "Provisioned": {
                "BrokerNodeGroupInfo": {"InstanceType": "kafka.m7g.large", "ClientSubnets": ["subnet-1"]},
                "CurrentBrokerSoftwareInfo": {"KafkaVersion": "3.6.0"},
                "NumberOfBrokerNodes": 3,
                "ClientAuthentication": {"Sasl": {"Iam": {"Enabled": True}, "Scram": {"Enabled": False}}},
                "EncryptionInfo": {"EncryptionInTransit": {"ClientBroker": "TLS", "InCluster": True}},
                "EnhancedMonitoring": "DEFAULT",
            },
        }
    }


BOOTSTRAP = {
    "BootstrapBrokerStringSaslIam": "b-1.orders.kafka.us-east-1.amazonaws.com:9098,b-2.orders:9098",
    "BootstrapBrokerStringTls": "b-1.orders.kafka.us-east-1.amazonaws.com:9094",
}
