"""Amazon MSK control-plane calls (boto3 ``kafka`` client)."""

from __future__ import annotations

from typing import Any

from .errors import ConfigError, translate_aws_error
from .models import ClusterInfo, ClusterSummary

# GetBootstrapBrokers response keys, in order of preference for IAM auth.
_IAM_ENDPOINT_KEYS = (
    "BootstrapBrokerStringSaslIam",
    "BootstrapBrokerStringPublicSaslIam",
    "BootstrapBrokerStringVpcConnectivitySaslIam",
)
_ENDPOINT_LABELS = {
    "BootstrapBrokerStringSaslIam": "SASL/IAM (private)",
    "BootstrapBrokerStringPublicSaslIam": "SASL/IAM (public)",
    "BootstrapBrokerStringVpcConnectivitySaslIam": "SASL/IAM (multi-VPC)",
    "BootstrapBrokerStringSaslScram": "SASL/SCRAM (private)",
    "BootstrapBrokerStringPublicSaslScram": "SASL/SCRAM (public)",
    "BootstrapBrokerStringTls": "TLS (private)",
    "BootstrapBrokerStringPublicTls": "TLS (public)",
    "BootstrapBrokerString": "Plaintext (private)",
}


class MskClient:
    def __init__(self, client: Any) -> None:
        self._client = client

    def list_clusters(self) -> list[ClusterSummary]:
        try:
            paginator = self._client.get_paginator("list_clusters_v2")
            clusters: list[ClusterSummary] = []
            for page in paginator.paginate():
                for info in page.get("ClusterInfoList", []):
                    clusters.append(_summary(info))
        except Exception as exc:
            raise translate_aws_error(exc, "kafka:ListClustersV2") from exc
        return sorted(clusters, key=lambda c: c.name.lower())

    def describe_cluster(self, arn: str) -> ClusterInfo:
        try:
            info = self._client.describe_cluster_v2(ClusterArn=arn)["ClusterInfo"]
        except Exception as exc:
            raise translate_aws_error(exc, "kafka:DescribeClusterV2", arn) from exc
        cluster = _info(info)
        cluster.endpoints = self.bootstrap_endpoints(arn)
        return cluster

    def bootstrap_endpoints(self, arn: str) -> dict[str, str]:
        try:
            resp = self._client.get_bootstrap_brokers(ClusterArn=arn)
        except Exception as exc:
            raise translate_aws_error(exc, "kafka:GetBootstrapBrokers", arn) from exc
        return {key: value for key, value in resp.items() if key.startswith("BootstrapBroker") and value}

    def iam_bootstrap(self, arn: str) -> str:
        endpoints = self.bootstrap_endpoints(arn)
        for key in _IAM_ENDPOINT_KEYS:
            if endpoints.get(key):
                return endpoints[key]
        raise ConfigError(
            "The cluster has no SASL/IAM bootstrap endpoint, so Fulcrum cannot connect to its brokers.",
            hint="Enable IAM access control on the cluster (Security settings → IAM role-based authentication).",
        )


def endpoint_label(key: str) -> str:
    return _ENDPOINT_LABELS.get(key, key)


def _summary(info: dict[str, Any]) -> ClusterSummary:
    provisioned = info.get("Provisioned") or {}
    return ClusterSummary(
        arn=info["ClusterArn"],
        name=info.get("ClusterName", ""),
        cluster_type=info.get("ClusterType", "PROVISIONED"),
        state=info.get("State", "UNKNOWN"),
        kafka_version=(provisioned.get("CurrentBrokerSoftwareInfo") or {}).get("KafkaVersion"),
    )


def _auth_types(client_auth: dict[str, Any]) -> list[str]:
    types: list[str] = []
    sasl = client_auth.get("Sasl") or {}
    if (sasl.get("Iam") or {}).get("Enabled"):
        types.append("SASL/IAM")
    if (sasl.get("Scram") or {}).get("Enabled"):
        types.append("SASL/SCRAM")
    if (client_auth.get("Tls") or {}).get("Enabled"):
        types.append("mTLS")
    if (client_auth.get("Unauthenticated") or {}).get("Enabled"):
        types.append("Unauthenticated")
    return types


def _info(info: dict[str, Any]) -> ClusterInfo:
    summary = _summary(info)
    provisioned = info.get("Provisioned") or {}
    serverless = info.get("Serverless") or {}
    client_auth = provisioned.get("ClientAuthentication") or serverless.get("ClientAuthentication") or {}
    in_transit = ((provisioned.get("EncryptionInfo") or {}).get("EncryptionInTransit")) or {}
    created = info.get("CreationTime")
    return ClusterInfo(
        **summary.model_dump(),
        broker_count=provisioned.get("NumberOfBrokerNodes"),
        broker_instance_type=(provisioned.get("BrokerNodeGroupInfo") or {}).get("InstanceType"),
        auth_types=_auth_types(client_auth),
        encryption_in_transit=in_transit.get("ClientBroker"),
        enhanced_monitoring=provisioned.get("EnhancedMonitoring"),
        created=created.isoformat() if hasattr(created, "isoformat") else (str(created) if created else None),
    )
