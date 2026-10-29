"""User-facing error types and translation of AWS / Kafka failures into actionable messages."""

from __future__ import annotations

import concurrent.futures
from typing import Any

from botocore.exceptions import (
    BotoCoreError,
    ClientError,
    ConnectTimeoutError,
    EndpointConnectionError,
    NoCredentialsError,
    NoRegionError,
    PartialCredentialsError,
    ReadTimeoutError,
)

NETWORK_HINT = (
    "MSK brokers usually live in a private VPC. The machine running Claude Code needs a network "
    "path to them: connect a VPN, tunnel through a bastion host, or run Claude Code inside the VPC. "
    "Also check that the cluster's security group allows inbound TCP 9098 (IAM, private) or "
    "9198 (IAM, public) from your address."
)


class FulcrumError(Exception):
    """Base class for errors whose message is safe and useful to show the user."""

    title = "Fulcrum error"

    def __init__(self, message: str, hint: str | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.hint = hint

    def render(self) -> str:
        text = f"**{self.title}:** {self.message}"
        if self.hint:
            text += f"\n\n{self.hint}"
        return text


class ConfigError(FulcrumError):
    title = "Configuration error"


class InputValidationError(FulcrumError):
    title = "Invalid input"


class CredentialsError(FulcrumError):
    title = "AWS credentials not found"


class AccessDeniedError(FulcrumError):
    title = "Access denied"

    def __init__(self, action: str, detail: str | None = None) -> None:
        self.action = action
        message = f"The current AWS identity is not allowed to perform `{action}`."
        if detail:
            message += f" ({detail})"
        super().__init__(
            message,
            hint=(
                f"Add `{action}` to the identity's IAM policy. "
                "See `iam/fulcrum-readonly-policy.json` in the plugin for the full least-privilege policy."
            ),
        )


class NetworkError(FulcrumError):
    title = "Network unreachable"

    def __init__(self, message: str) -> None:
        super().__init__(message, hint=NETWORK_HINT)


class NotFoundError(FulcrumError):
    title = "Not found"


class UnsupportedError(FulcrumError):
    title = "Not supported"


_ACCESS_DENIED_CODES = {
    "AccessDenied",
    "AccessDeniedException",
    "UnauthorizedOperation",
    "UnauthorizedException",
    "ForbiddenException",
}
_NOT_FOUND_CODES = {"NotFoundException", "EntityNotFoundException", "ResourceNotFoundException"}
_CREDENTIAL_CODES = {"ExpiredToken", "ExpiredTokenException", "InvalidClientTokenId", "UnrecognizedClientException"}


def translate_aws_error(exc: Exception, action: str, resource: str | None = None) -> FulcrumError:
    """Convert a boto3/botocore exception raised while calling ``action`` into a FulcrumError."""
    if isinstance(exc, FulcrumError):
        return exc
    if isinstance(exc, (NoCredentialsError, PartialCredentialsError)):
        return CredentialsError(
            "No usable AWS credentials were found in the standard credential chain.",
            hint=(
                "Set AWS_PROFILE (and run `aws sso login --profile <name>` for SSO profiles), export "
                "AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY, or run where an instance/task role is available. "
                "Fulcrum never accepts or stores secrets itself."
            ),
        )
    if isinstance(exc, NoRegionError):
        return ConfigError("No AWS region configured.", hint="Set AWS_REGION or the plugin's `aws_region` option.")
    if isinstance(exc, ClientError):
        error: dict[str, Any] = exc.response.get("Error", {})
        code = str(error.get("Code", ""))
        msg = str(error.get("Message", ""))
        if code in _ACCESS_DENIED_CODES:
            return AccessDeniedError(action, msg or None)
        if code in _NOT_FOUND_CODES:
            what = f"`{resource}`" if resource else "The requested resource"
            return NotFoundError(f"{what} was not found ({action}: {msg}).")
        if code in _CREDENTIAL_CODES:
            return CredentialsError(
                f"AWS rejected the credentials: {msg or code}.",
                hint="Refresh them (for SSO: `aws sso login --profile <name>`) and retry.",
            )
        return FulcrumError(f"{action} failed: {code}: {msg}")
    if isinstance(exc, (EndpointConnectionError, ConnectTimeoutError, ReadTimeoutError)):
        return FulcrumError(
            f"Could not reach the AWS API endpoint for {action}: {exc}",
            hint="Check internet/proxy access to AWS APIs and that AWS_REGION is correct.",
        )
    if isinstance(exc, BotoCoreError):
        # Covers SSO token expiry (TokenRetrievalError, SSOTokenLoadError, ...).
        name = type(exc).__name__
        if "Token" in name or "SSO" in name:
            return CredentialsError(f"{name}: {exc}", hint="Run `aws sso login --profile <name>` and retry.")
        return FulcrumError(f"{action} failed: {exc}")
    return FulcrumError(f"{action} failed: {type(exc).__name__}: {exc}")


# librdkafka error names (KafkaError.name()) mapped to the MSK IAM action that usually fixes them.
_KAFKA_AUTHZ_ACTIONS = {
    "TOPIC_AUTHORIZATION_FAILED": "kafka-cluster:DescribeTopic",
    "GROUP_AUTHORIZATION_FAILED": "kafka-cluster:DescribeGroup",
    "CLUSTER_AUTHORIZATION_FAILED": "kafka-cluster:DescribeCluster",
    "TRANSACTIONAL_ID_AUTHORIZATION_FAILED": "kafka-cluster:DescribeTransactionalId",
}
_KAFKA_NETWORK = {"_TRANSPORT", "_TIMED_OUT", "_ALL_BROKERS_DOWN", "_RESOLVE", "REQUEST_TIMED_OUT", "_SSL"}
_KAFKA_NOT_FOUND = {"UNKNOWN_TOPIC_OR_PART", "GROUP_ID_NOT_FOUND", "_UNKNOWN_TOPIC", "_UNKNOWN_PARTITION"}


def translate_kafka_error(exc: BaseException, operation: str, iam_action: str | None = None) -> FulcrumError:
    """Convert a confluent-kafka / futures exception into a FulcrumError."""
    if isinstance(exc, FulcrumError):
        return exc
    if isinstance(exc, (concurrent.futures.TimeoutError, TimeoutError)):
        return NetworkError(f"Timed out waiting for the Kafka brokers during {operation}.")

    kerr = exc.args[0] if getattr(exc, "args", None) else None
    name = ""
    detail = str(exc)
    if kerr is not None and hasattr(kerr, "name"):
        try:
            name = str(kerr.name())
            detail = str(kerr.str())
        except Exception:  # pragma: no cover - defensive
            name = ""
    if name in _KAFKA_AUTHZ_ACTIONS:
        # The error code names the exact resource type that was denied, so it beats the caller's guess.
        return AccessDeniedError(_KAFKA_AUTHZ_ACTIONS[name], detail)
    if "AUTHORIZATION_FAILED" in name:
        return AccessDeniedError(iam_action or "kafka-cluster:DescribeCluster", detail)
    if name in {"SASL_AUTHENTICATION_FAILED", "_AUTHENTICATION"}:
        return AccessDeniedError(
            "kafka-cluster:Connect",
            f"SASL/IAM authentication failed: {detail}. Also confirm the AWS credentials are valid.",
        )
    if name in _KAFKA_NETWORK:
        return NetworkError(f"Could not reach the Kafka brokers during {operation}: {detail}")
    if name in _KAFKA_NOT_FOUND:
        return NotFoundError(f"{operation}: {detail}")
    return FulcrumError(f"{operation} failed: {detail}")
