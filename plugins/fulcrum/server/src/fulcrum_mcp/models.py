"""Typed models for data returned by the AWS and Kafka wrappers."""

from __future__ import annotations

from pydantic import BaseModel, Field


class ClusterSummary(BaseModel):
    arn: str
    name: str
    cluster_type: str
    state: str
    kafka_version: str | None = None


class ClusterInfo(BaseModel):
    arn: str
    name: str
    cluster_type: str
    state: str
    kafka_version: str | None = None
    broker_count: int | None = None
    broker_instance_type: str | None = None
    auth_types: list[str] = Field(default_factory=list)
    encryption_in_transit: str | None = None
    enhanced_monitoring: str | None = None
    created: str | None = None
    endpoints: dict[str, str] = Field(default_factory=dict)


class TopicSummary(BaseModel):
    name: str
    partitions: int
    replication_factor: int
    internal: bool


class PartitionInfo(BaseModel):
    partition: int
    leader: int | None
    replicas: list[int]
    isr: list[int]
    earliest_offset: int | None = None
    latest_offset: int | None = None

    @property
    def under_replicated(self) -> bool:
        return len(self.isr) < len(self.replicas)

    @property
    def offline(self) -> bool:
        return self.leader is None or self.leader < 0


class TopicDetail(BaseModel):
    name: str
    internal: bool
    partitions: list[PartitionInfo]
    configs: dict[str, str] = Field(default_factory=dict)
    size_bytes: float | None = None


class GroupSummary(BaseModel):
    group_id: str
    state: str
    members: int
    group_type: str | None = None
    simple: bool = False


class GroupMember(BaseModel):
    member_id: str
    client_id: str
    host: str
    group_instance_id: str | None = None
    assignments: list[tuple[str, int]] = Field(default_factory=list)


class PartitionLag(BaseModel):
    topic: str
    partition: int
    committed: int | None
    end_offset: int | None
    owner: str | None = None

    @property
    def lag(self) -> int | None:
        if self.committed is None or self.end_offset is None or self.committed < 0:
            return None
        return max(self.end_offset - self.committed, 0)


class GroupDetail(BaseModel):
    group_id: str
    state: str
    group_type: str | None = None
    partition_assignor: str | None = None
    coordinator: int | None = None
    members: list[GroupMember] = Field(default_factory=list)
    partitions: list[PartitionLag] = Field(default_factory=list)

    @property
    def total_lag(self) -> int:
        return sum(p.lag or 0 for p in self.partitions)


class ConsumerClient(BaseModel):
    group_id: str
    member_id: str
    client_id: str
    host: str
    assignments: list[tuple[str, int]] = Field(default_factory=list)


class RegistryInfo(BaseModel):
    name: str
    arn: str | None = None
    status: str | None = None
    description: str | None = None


class SchemaSummary(BaseModel):
    registry: str
    name: str
    status: str | None = None
    data_format: str | None = None
    compatibility: str | None = None
    latest_version: int | None = None
    updated: str | None = None


class SchemaVersionInfo(BaseModel):
    version: int
    status: str | None = None
    created: str | None = None
    version_id: str | None = None


class SchemaVersionDetail(BaseModel):
    registry: str
    name: str
    version: int
    data_format: str | None = None
    status: str | None = None
    created: str | None = None
    definition: str


class SchemaDetail(BaseModel):
    summary: SchemaSummary
    description: str | None = None
    arn: str | None = None
    versions: list[SchemaVersionInfo] = Field(default_factory=list)
    current: SchemaVersionDetail | None = None


class TopicThroughput(BaseModel):
    topic: str
    bytes_in_per_sec: float | None = None
    messages_in_per_sec: float | None = None


class GroupLag(BaseModel):
    group_id: str
    state: str
    total_lag: int
    max_partition_lag: int
    partitions_lagging: int
