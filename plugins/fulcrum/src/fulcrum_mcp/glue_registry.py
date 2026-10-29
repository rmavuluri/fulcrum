"""AWS Glue Schema Registry reads (boto3 ``glue`` client)."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import Any

from .errors import InputValidationError, NotFoundError, translate_aws_error
from .models import (
    RegistryInfo,
    SchemaDetail,
    SchemaSummary,
    SchemaVersionDetail,
    SchemaVersionInfo,
)


def _ts(value: Any) -> str | None:
    if value is None:
        return None
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


class GlueRegistry:
    def __init__(self, client: Any, default_registry: str | None = None) -> None:
        self._client = client
        self.default_registry = default_registry

    def list_registries(self) -> list[RegistryInfo]:
        try:
            registries = [
                RegistryInfo(
                    name=r["RegistryName"],
                    arn=r.get("RegistryArn"),
                    status=r.get("Status"),
                    description=r.get("Description"),
                )
                for page in self._client.get_paginator("list_registries").paginate()
                for r in page.get("Registries", [])
            ]
        except Exception as exc:
            raise translate_aws_error(exc, "glue:ListRegistries") from exc
        return sorted(registries, key=lambda r: r.name.lower())

    def _registry_names(self, registry: str | None) -> list[str]:
        if registry:
            return [registry]
        if self.default_registry:
            return [self.default_registry]
        return [r.name for r in self.list_registries()]

    def _list_schemas_in(self, registry: str) -> list[dict[str, Any]]:
        try:
            paginator = self._client.get_paginator("list_schemas")
            return [
                s for page in paginator.paginate(RegistryId={"RegistryName": registry}) for s in page.get("Schemas", [])
            ]
        except Exception as exc:
            raise translate_aws_error(exc, "glue:ListSchemas", registry) from exc

    def list_schemas(
        self, registry: str | None = None, prefix: str | None = None, known_registries: list[str] | None = None
    ) -> list[SchemaSummary]:
        """List schemas in one registry, the default registry, or every registry.
        ``known_registries`` lets callers that already listed registries skip a second ListRegistries."""
        if registry or self.default_registry or known_registries is None:
            names = self._registry_names(registry)
        else:
            names = known_registries
        with ThreadPoolExecutor(max_workers=min(8, max(1, len(names)))) as pool:
            per_registry = list(pool.map(self._list_schemas_in, names))
        schemas = [
            SchemaSummary(
                registry=s.get("RegistryName", reg),
                name=s["SchemaName"],
                status=s.get("SchemaStatus"),
                updated=_ts(s.get("UpdatedTime")),
            )
            for reg, items in zip(names, per_registry, strict=True)
            for s in items
            if not prefix or s["SchemaName"].startswith(prefix)
        ]
        return sorted(schemas, key=lambda s: (s.registry.lower(), s.name.lower()))

    def resolve_registry(self, name: str, registry: str | None) -> str:
        """Find which registry holds schema ``name`` when the caller did not say."""
        if registry:
            return registry
        if self.default_registry:
            return self.default_registry
        matches = sorted({s.registry for s in self.list_schemas() if s.name == name})
        if not matches:
            raise NotFoundError(f"No schema named `{name}` exists in any Glue registry in this region.")
        if len(matches) > 1:
            raise InputValidationError(
                f"Schema `{name}` exists in several registries: {', '.join(matches)}.",
                hint="Pass `registry=<name>` to choose one.",
            )
        return matches[0]

    def get_schema(self, name: str, registry: str) -> dict[str, Any]:
        try:
            return self._client.get_schema(SchemaId={"RegistryName": registry, "SchemaName": name})
        except Exception as exc:
            raise translate_aws_error(exc, "glue:GetSchema", f"{registry}/{name}") from exc

    def list_versions(self, name: str, registry: str) -> list[SchemaVersionInfo]:
        try:
            paginator = self._client.get_paginator("list_schema_versions")
            versions = [
                SchemaVersionInfo(
                    version=v["VersionNumber"],
                    status=v.get("Status"),
                    created=_ts(v.get("CreatedTime")),
                    version_id=v.get("SchemaVersionId"),
                )
                for page in paginator.paginate(SchemaId={"RegistryName": registry, "SchemaName": name})
                for v in page.get("Schemas", [])
            ]
        except Exception as exc:
            raise translate_aws_error(exc, "glue:ListSchemaVersions", f"{registry}/{name}") from exc
        return sorted(versions, key=lambda v: v.version)

    def get_version(self, name: str, registry: str, version: int | None) -> SchemaVersionDetail:
        number: dict[str, Any] = {"LatestVersion": True} if version is None else {"VersionNumber": version}
        label = f"{registry}/{name} v{version or 'latest'}"
        try:
            resp = self._client.get_schema_version(
                SchemaId={"RegistryName": registry, "SchemaName": name}, SchemaVersionNumber=number
            )
        except Exception as exc:
            raise translate_aws_error(exc, "glue:GetSchemaVersion", label) from exc
        return SchemaVersionDetail(
            registry=registry,
            name=name,
            version=resp.get("VersionNumber", version or 0),
            data_format=resp.get("DataFormat"),
            status=resp.get("Status"),
            created=_ts(resp.get("CreatedTime")),
            definition=resp.get("SchemaDefinition", ""),
        )

    def describe(self, name: str, registry: str | None, version: int | None) -> SchemaDetail:
        reg = self.resolve_registry(name, registry)
        meta = self.get_schema(name, reg)
        versions = self.list_versions(name, reg)
        current = self.get_version(name, reg, version)
        summary = SchemaSummary(
            registry=reg,
            name=name,
            status=meta.get("SchemaStatus"),
            data_format=meta.get("DataFormat"),
            compatibility=meta.get("Compatibility"),
            latest_version=meta.get("LatestSchemaVersion"),
            updated=_ts(meta.get("UpdatedTime")),
        )
        return SchemaDetail(
            summary=summary,
            description=meta.get("Description"),
            arn=meta.get("SchemaArn"),
            versions=versions,
            current=current,
        )
