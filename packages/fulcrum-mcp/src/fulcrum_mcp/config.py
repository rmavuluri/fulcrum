"""Configuration loading and validation.

Values come from two places, highest precedence first:

1. Plugin ``userConfig`` options, which ``.mcp.json`` passes in as ``FULCRUM_OPT_*`` variables.
2. Conventional environment variables (``AWS_REGION``, ``MSK_CLUSTER_ARN``, ...).

An unset ``userConfig`` option may arrive as an empty string or as an unexpanded ``${...}``
placeholder; both are treated as "not set" so the environment variable fallback applies.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

from .errors import ConfigError
from .validation import (
    region_from_bootstrap,
    validate_bootstrap_brokers,
    validate_cluster_arn,
    validate_external_id,
    validate_registry_name,
    validate_role_arn,
)

DEFAULT_LAG_THRESHOLD = 1000
DEFAULT_CACHE_TTL = 30.0
DEFAULT_TIMEOUT = 15.0
DEFAULT_MAX_ROWS = 100


@dataclass(frozen=True)
class Settings:
    region: str | None
    profile: str | None
    cluster_arn: str | None
    registry_name: str | None
    bootstrap_brokers: str | None = None
    role_arn: str | None = None
    role_external_id: str | None = None
    lag_threshold: int = DEFAULT_LAG_THRESHOLD
    cache_ttl: float = DEFAULT_CACHE_TTL
    timeout: float = DEFAULT_TIMEOUT
    max_rows: int = DEFAULT_MAX_ROWS
    log_level: str = "INFO"


def _pick(env: Mapping[str, str], *names: str) -> str | None:
    for name in names:
        value = env.get(name)
        if value is None:
            continue
        value = value.strip()
        if not value or value.startswith("${"):
            continue
        return value
    return None


def _number(env: Mapping[str, str], names: tuple[str, ...], default: float, minimum: float, kind: type) -> float:
    raw = _pick(env, *names)
    if raw is None:
        return default
    try:
        value = kind(raw)
    except ValueError as exc:
        raise ConfigError(f"{names[-1]} must be a {kind.__name__}, got {raw!r}.") from exc
    if value < minimum:
        raise ConfigError(f"{names[-1]} must be >= {minimum}, got {raw!r}.")
    return value


def load_settings(env: Mapping[str, str] | None = None) -> Settings:
    """Build validated Settings from the environment."""
    env = os.environ if env is None else env
    cluster_arn = _pick(env, "FULCRUM_OPT_MSK_CLUSTER_ARN", "MSK_CLUSTER_ARN")
    if cluster_arn:
        cluster_arn = validate_cluster_arn(cluster_arn)
    registry = _pick(env, "FULCRUM_OPT_GLUE_REGISTRY_NAME", "GLUE_REGISTRY_NAME")
    if registry:
        registry = validate_registry_name(registry)
    brokers = _pick(env, "FULCRUM_OPT_BOOTSTRAP_BROKERS", "MSK_BOOTSTRAP_BROKERS")
    if brokers:
        brokers = validate_bootstrap_brokers(brokers)
    role_arn = _pick(env, "FULCRUM_OPT_IAM_ROLE_ARN", "FULCRUM_IAM_ROLE_ARN")
    if role_arn:
        role_arn = validate_role_arn(role_arn)
    external_id = _pick(env, "FULCRUM_OPT_IAM_ROLE_EXTERNAL_ID", "FULCRUM_IAM_ROLE_EXTERNAL_ID")
    if external_id:
        if not role_arn:
            raise ConfigError("An IAM role external ID is set but no IAM role ARN is configured.")
        external_id = validate_external_id(external_id)

    # Region precedence: explicit setting, then cluster ARN, then broker hostnames.
    # (The AWS profile's own region is the last fallback, applied lazily by the service.)
    region = _pick(env, "FULCRUM_OPT_AWS_REGION", "AWS_REGION", "AWS_DEFAULT_REGION")
    if region is None and cluster_arn:
        region = cluster_arn.split(":")[3]
    if region is None and brokers:
        region = region_from_bootstrap(brokers)
    return Settings(
        region=region,
        profile=_pick(env, "FULCRUM_OPT_AWS_PROFILE", "AWS_PROFILE"),
        cluster_arn=cluster_arn,
        registry_name=registry,
        bootstrap_brokers=brokers,
        role_arn=role_arn,
        role_external_id=external_id,
        lag_threshold=int(
            _number(env, ("FULCRUM_OPT_LAG_THRESHOLD", "FULCRUM_LAG_THRESHOLD"), DEFAULT_LAG_THRESHOLD, 0, int)
        ),
        cache_ttl=float(_number(env, ("FULCRUM_OPT_CACHE_TTL", "FULCRUM_CACHE_TTL"), DEFAULT_CACHE_TTL, 0, float)),
        timeout=float(_number(env, ("FULCRUM_TIMEOUT",), DEFAULT_TIMEOUT, 1, float)),
        max_rows=int(_number(env, ("FULCRUM_MAX_ROWS",), DEFAULT_MAX_ROWS, 10, int)),
        log_level=(_pick(env, "FULCRUM_LOG_LEVEL") or "INFO").upper(),
    )
