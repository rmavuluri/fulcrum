# Changelog

All notable changes to this plugin are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/). The version in
`.claude-plugin/plugin.json` and `server/pyproject.toml` must match the newest entry.

## [Unreleased]

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

[Unreleased]: https://github.com/rmavuluri/fulcrum/compare/fulcrum-v0.1.0...HEAD
[0.1.0]: https://github.com/rmavuluri/fulcrum/releases/tag/fulcrum-v0.1.0
