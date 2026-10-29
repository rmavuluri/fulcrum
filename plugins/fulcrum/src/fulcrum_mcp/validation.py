"""Input validation for names passed in from the model or the user."""

from __future__ import annotations

import re

from .errors import InputValidationError

_TOPIC_RE = re.compile(r"^[A-Za-z0-9._-]{1,249}$")
_PREFIX_RE = re.compile(r"^[A-Za-z0-9._$#-]{0,255}$")
_GLUE_NAME_RE = re.compile(r"^[A-Za-z0-9_$#.-]{1,255}$")
_CLUSTER_ARN_RE = re.compile(r"^arn:aws[a-z-]*:kafka:[a-z0-9-]+:\d{12}:cluster/[A-Za-z0-9-]+/[A-Za-z0-9-]+$")
_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]")


def validate_topic(name: str) -> str:
    name = (name or "").strip()
    if not _TOPIC_RE.match(name) or name in {".", ".."}:
        raise InputValidationError(
            f"{name!r} is not a valid Kafka topic name (1-249 chars of letters, digits, '.', '_', '-')."
        )
    return name


def validate_group_id(group_id: str) -> str:
    group_id = (group_id or "").strip()
    if not group_id or len(group_id) > 255 or _CONTROL_RE.search(group_id):
        raise InputValidationError(f"{group_id!r} is not a valid consumer group id (1-255 printable chars).")
    return group_id


def validate_prefix(prefix: str | None) -> str | None:
    if prefix is None:
        return None
    prefix = prefix.strip()
    if not _PREFIX_RE.match(prefix):
        raise InputValidationError(f"{prefix!r} is not a valid name prefix.")
    return prefix or None


def validate_schema_name(name: str) -> str:
    name = (name or "").strip()
    if not _GLUE_NAME_RE.match(name):
        raise InputValidationError(f"{name!r} is not a valid Glue schema name.")
    return name


def validate_registry_name(name: str) -> str:
    name = (name or "").strip()
    if not _GLUE_NAME_RE.match(name):
        raise InputValidationError(f"{name!r} is not a valid Glue registry name.")
    return name


def validate_cluster_arn(arn: str) -> str:
    arn = (arn or "").strip()
    if not _CLUSTER_ARN_RE.match(arn):
        raise InputValidationError(
            f"{arn!r} is not a valid MSK cluster ARN (expected arn:aws:kafka:<region>:<account>:cluster/<name>/<uuid>)."
        )
    return arn


def validate_version(version: int | str | None) -> int | None:
    if version is None or version == "" or str(version).lower() == "latest":
        return None
    try:
        number = int(str(version).lstrip("vV"))
    except ValueError as exc:
        raise InputValidationError(f"{version!r} is not a valid schema version number.") from exc
    if number < 1:
        raise InputValidationError("Schema versions start at 1.")
    return number


def validate_page(limit: int, offset: int, max_limit: int = 1000) -> tuple[int, int]:
    if limit < 1 or limit > max_limit:
        raise InputValidationError(f"limit must be between 1 and {max_limit}.")
    if offset < 0:
        raise InputValidationError("offset must be >= 0.")
    return limit, offset


_HOST_RE = re.compile(
    r"^(?=.{1,253}$)[A-Za-z0-9](?:[A-Za-z0-9-]{0,62}[A-Za-z0-9])?(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,62}[A-Za-z0-9])?)*$"
)
_ROLE_ARN_RE = re.compile(r"^arn:aws[a-z-]*:iam::\d{12}:role/[\w+=,.@/-]{1,512}$")
_EXTERNAL_ID_RE = re.compile(r"^[\w+=,.@:/-]{2,1224}$")
_MSK_HOST_REGION_RE = re.compile(r"\.kafka(?:-serverless)?\.([a-z]{2}(?:-[a-z]+)+-\d)\.amazonaws\.com(?:\.cn)?$")


def validate_bootstrap_brokers(value: str) -> str:
    """Validate a comma-separated ``host:port`` list, e.g. the SASL/IAM bootstrap string."""
    entries = [e.strip() for e in (value or "").split(",") if e.strip()]
    if not entries or len(entries) > 30:
        raise InputValidationError("Bootstrap brokers must be 1-30 comma-separated `host:port` entries.")
    for entry in entries:
        host, sep, port = entry.rpartition(":")
        if not sep or not _HOST_RE.match(host) or not port.isdigit() or not 0 < int(port) < 65536:
            raise InputValidationError(
                f"{entry!r} is not a valid broker address (expected host:port, "
                "e.g. b-1.mycluster.abc123.c2.kafka.us-east-1.amazonaws.com:9098)."
            )
    return ",".join(entries)


def region_from_bootstrap(value: str) -> str | None:
    """Infer the AWS region from MSK broker hostnames (``...kafka.<region>.amazonaws.com``)."""
    for entry in value.split(","):
        match = _MSK_HOST_REGION_RE.search(entry.rpartition(":")[0])
        if match:
            return match.group(1)
    return None


def validate_role_arn(arn: str) -> str:
    arn = (arn or "").strip()
    if not _ROLE_ARN_RE.match(arn):
        raise InputValidationError(
            f"{arn!r} is not a valid IAM role ARN (expected arn:aws:iam::<account>:role/<name>)."
        )
    return arn


def validate_external_id(value: str) -> str:
    value = (value or "").strip()
    if not _EXTERNAL_ID_RE.match(value):
        raise InputValidationError("The external ID must be 2-1224 characters of letters, digits and +=,.@:/-_.")
    return value
