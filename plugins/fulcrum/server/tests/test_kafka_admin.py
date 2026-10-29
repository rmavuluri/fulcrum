import pytest
from confluent_kafka import KafkaError

from fulcrum_mcp.errors import AccessDeniedError, NetworkError, NotFoundError
from fulcrum_mcp.kafka_admin import KafkaAdmin, is_internal_topic

from .conftest import failed, kafka_exc

READ_ONLY_METHODS = {
    "poll",
    "list_topics",
    "describe_cluster",
    "describe_topics",
    "describe_configs",
    "list_offsets",
    "list_consumer_groups",
    "describe_consumer_groups",
    "list_consumer_group_offsets",
}


def test_client_config_and_single_instance(fake_admin_client):
    created = []

    def factory(conf):
        created.append(conf)
        return fake_admin_client

    admin = KafkaAdmin("b-1:9098", lambda _c: ("t", 0.0), timeout=3, admin_factory=factory)
    admin.list_topics()
    admin.list_groups()
    assert len(created) == 1, "the AdminClient must be created once and reused"
    conf = created[0]
    assert conf["security.protocol"] == "SASL_SSL"
    assert conf["sasl.mechanism"] == "OAUTHBEARER"
    assert callable(conf["oauth_cb"])
    assert fake_admin_client.polled


def test_only_read_only_admin_calls(kafka_admin, fake_admin_client):
    kafka_admin.list_topics()
    kafka_admin.describe_topic("orders")
    kafka_admin.list_groups()
    kafka_admin.describe_group("orders-consumer")
    kafka_admin.list_consumers()
    kafka_admin.group_lags()
    kafka_admin.describe_cluster()
    kafka_admin.partition_health()
    assert set(fake_admin_client.calls) <= READ_ONLY_METHODS


def test_list_topics(kafka_admin):
    topics = {t.name: t for t in kafka_admin.list_topics()}
    assert list(topics) == sorted(topics)
    assert topics["orders"].partitions == 2 and topics["orders"].replication_factor == 3
    assert topics["__consumer_offsets"].internal and not topics["orders"].internal
    assert is_internal_topic("__amazon_msk_canary")


def test_describe_topic(kafka_admin):
    detail = kafka_admin.describe_topic("orders")
    p0, p1 = detail.partitions
    assert (p0.earliest_offset, p0.latest_offset) == (10, 110)
    assert p1.under_replicated and not p0.under_replicated
    assert detail.configs["min.insync.replicas"] == "2"  # override
    assert "retention.ms" in detail.configs  # key setting at default
    assert "segment.bytes" not in detail.configs  # boring default hidden
    assert "sasl.secret" not in detail.configs  # sensitive never shown


def test_describe_topic_offline_and_missing(kafka_admin):
    assert kafka_admin.describe_topic("broken").partitions[0].offline
    with pytest.raises(NotFoundError):
        kafka_admin.describe_topic("nope")


def test_describe_topic_config_failure_is_soft(kafka_admin, fake_admin_client):
    fake_admin_client.describe_configs = lambda resources, request_timeout=0: {
        r: failed(kafka_exc(KafkaError.TOPIC_AUTHORIZATION_FAILED)) for r in resources
    }
    detail = kafka_admin.describe_topic("orders")
    assert "(unavailable)" in detail.configs


def test_groups_and_consumers(kafka_admin, fake_admin_client):
    groups = {g.group_id: g for g in kafka_admin.list_groups()}
    assert groups["orders-consumer"].state == "STABLE" and groups["orders-consumer"].members == 1
    assert groups["payments-audit"].state == "EMPTY"
    consumers = kafka_admin.list_consumers()
    assert len(consumers) == 1 and consumers[0].assignments == [("orders", 0), ("orders", 1)]

    fake_admin_client.describe_group_errors["payments-audit"] = kafka_exc(KafkaError.GROUP_AUTHORIZATION_FAILED)
    groups = {g.group_id: g for g in kafka_admin.list_groups()}
    assert groups["payments-audit"].members == 0  # falls back to the listing
    assert len(kafka_admin.list_consumers()) == 1


def test_describe_group_lag(kafka_admin):
    detail = kafka_admin.describe_group("orders-consumer")
    lags = {(p.topic, p.partition): p.lag for p in detail.partitions}
    assert lags == {("orders", 0): 10, ("orders", 1): 0}
    assert detail.total_lag == 10
    assert detail.partitions[0].owner == "orders-service"


def test_describe_group_missing_and_denied(kafka_admin, fake_admin_client):
    with pytest.raises(NotFoundError):
        kafka_admin.describe_group("ghost")
    fake_admin_client.describe_group_errors["orders-consumer"] = kafka_exc(KafkaError.GROUP_AUTHORIZATION_FAILED)
    with pytest.raises(AccessDeniedError, match="DescribeGroup"):
        kafka_admin.describe_group("orders-consumer")


def test_group_lags_batch(kafka_admin, fake_admin_client):
    lags = kafka_admin.group_lags()
    assert [g.group_id for g in lags] == ["payments-audit", "orders-consumer"]  # sorted by lag desc
    assert lags[0].total_lag == 4000 and lags[0].partitions_lagging == 1
    assert fake_admin_client.calls.count("list_offsets") == 1, "end offsets must be fetched in one batch"


def test_partition_health_and_cluster(kafka_admin):
    health = kafka_admin.partition_health()
    offline = [(t, p.partition) for t, p in health if p.offline]
    urp = [(t, p.partition) for t, p in health if p.under_replicated and not p.offline]
    assert offline == [("broken", 0)]
    assert urp == [("orders", 1)]
    assert kafka_admin.broker_ids() == [1, 2, 3]
    assert kafka_admin.describe_cluster()["controller"] == 1


def test_unreachable_brokers_raise_network_error(kafka_admin, fake_admin_client):
    fake_admin_client.metadata_error = kafka_exc(KafkaError._TRANSPORT, "Failed to get metadata")
    with pytest.raises(NetworkError) as info:
        kafka_admin.list_topics()
    assert "VPC" in info.value.render()


def test_future_timeout_is_network_error(kafka_admin, fake_admin_client):
    from concurrent.futures import Future

    fake_admin_client.list_consumer_groups = lambda request_timeout=0: Future()  # never completes
    kafka_admin.timeout = 0.01
    with pytest.raises(NetworkError):
        kafka_admin.list_groups()
