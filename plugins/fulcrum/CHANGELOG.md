# Changelog

All notable changes to this plugin are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/). The version in
`.claude-plugin/plugin.json` and `pyproject.toml` must match the newest entry. The MCP server is
published separately to PyPI as `fulcrum-mcp`, and each plugin release pins one server version.

## [Unreleased]

## [0.3.0] - 2026-10-29

### Changed

- The MCP server is now published to PyPI as [`fulcrum-mcp`](https://pypi.org/project/fulcrum-mcp/)
  and the plugin starts it with `uvx fulcrum-mcp==0.3.0`. The plugin no longer contains Python
  source; its `uv.lock` pins the server and every dependency with hashes. This is the form the
  plugin directory can verify (Verified badge).
- The server's environment now lives in uv's tool cache instead of the plugin folder.

## [0.2.3] - 2026-10-29

### Changed

- The plugin is no longer built as a Python package on install: `pyproject.toml` has no build
  system, so `uv run` only installs the locked dependencies and no build backend is downloaded
  or run. `fulcrum_server.py` adds `src/` to the import path itself.

## [0.2.2] - 2026-10-29

### Added

- `PRIVACY.md`: what the plugin reads, where data goes, and how long it is kept.

### Changed

- `.mcp.json` now runs `fulcrum_server.py` from the plugin folder instead of the `fulcrum-mcp`
  console script, so the directory's validator can read the server's entry point.

## [0.2.1] - 2026-10-29

### Changed

- Packaging for the Claude plugin directory: the runtime `pyproject.toml` and `uv.lock` now sit
  next to `.mcp.json`, and the lock holds runtime dependencies only (dev tools and tests moved to
  the repository root, outside the plugin).
- The server's virtual environment now lives in the plugin folder (`.venv`) instead of the plugin
  data folder; it is rebuilt automatically on first start after updating.
- README: install uv with a package manager instead of a download-and-run script.
- CI now runs the tests on Linux, macOS and Windows.

## [0.2.0] - 2026-10-28

### Added

- **Direct broker endpoint:** new `bootstrap_brokers` option (env `MSK_BOOTSTRAP_BROKERS`). Fulcrum
  connects to the given SASL/IAM brokers with no MSK API lookup, so no cluster ARN or
  `kafka:GetBootstrapBrokers` permission is needed. The region is inferred from MSK hostnames.
  `cluster` and `health` work from broker data alone; adding the cluster ARN restores MSK state,
  version and CloudWatch producer metrics.
- **IAM role assumption:** new `iam_role_arn` and `iam_role_external_id` options (env
  `FULCRUM_IAM_ROLE_ARN`, `FULCRUM_IAM_ROLE_EXTERNAL_ID`). Base credentials are used only for
  `sts:AssumeRole`; every AWS call and every MSK IAM token is signed with the role's auto-refreshing
  temporary credentials. AssumeRole failures explain the trust-policy requirements.
- `iam/fulcrum-role-trust-policy.json` and `iam/fulcrum-assume-role-policy.json` templates.
- `/fulcrum status` shows the role, broker endpoint and connection mode.

### Changed

- MSK IAM tokens are now always signed from the same boto3 session as the AWS API calls.

## [0.1.0] - 2026-10-28

### Added

- `/fulcrum` command with subcommands `topics`, `topic`, `consumer-groups`, `consumer-group`,
  `consumers`, `producers`, `schemas`, `schema`, `schema-diff`, `cluster`, `clusters`,
  `health`, `status` and `help`.
- `kafka-insights` skill so natural-language Kafka and schema questions use the Fulcrum tools.
- Local read-only MCP server (`msk`) with 13 tools, built on the MCP Python SDK 2.x (`MCPServer`).
- MSK IAM auth (SASL_SSL + OAUTHBEARER) through `aws-msk-iam-sasl-signer-python`, with automatic
  token refresh, and one long-lived confluent-kafka `AdminClient` per cluster.
- Batched lag calculation: committed offsets for all groups requested concurrently, end offsets
  fetched in one request.
- Glue Schema Registry listing, schema details and field-level diffs for Avro, JSON Schema and Protobuf.
- Producer activity from CloudWatch per-topic ingress, with the Kafka limitation stated in the output.
- Plugin `userConfig` options with environment-variable fallbacks; short-TTL cache; paging and
  summarizing past 100 rows; `prefix=` filters.
- Clear errors for missing credentials, access denied (naming the missing IAM action), unreachable
  brokers (VPC guidance), and missing clusters, topics, groups or schemas.
- Least-privilege IAM policy, unit tests (moto, botocore Stubber, fake AdminClient) and an optional
  live integration test.

[Unreleased]: https://github.com/rmavuluri/fulcrum/compare/fulcrum-v0.3.0...HEAD
[0.3.0]: https://github.com/rmavuluri/fulcrum/releases/tag/fulcrum-v0.3.0
[0.2.3]: https://github.com/rmavuluri/fulcrum/releases/tag/fulcrum-v0.2.3
[0.2.2]: https://github.com/rmavuluri/fulcrum/releases/tag/fulcrum-v0.2.2
[0.2.1]: https://github.com/rmavuluri/fulcrum/releases/tag/fulcrum-v0.2.1
[0.2.0]: https://github.com/rmavuluri/fulcrum/releases/tag/fulcrum-v0.2.0
[0.1.0]: https://github.com/rmavuluri/fulcrum/releases/tag/fulcrum-v0.1.0
