# fulcrum

A Claude Code plugin marketplace with one plugin:

**[Fulcrum](plugins/fulcrum/README.md)** lets you query Amazon MSK (Kafka) and the AWS Glue Schema Registry from inside Claude Code, using IAM auth. It covers topics, consumer groups and lag, active consumers, producer activity, schemas and schema diffs, cluster info and health. It is read-only.

```
/plugin marketplace add rmavuluri/fulcrum
/plugin install fulcrum@fulcrum-marketplace
/fulcrum help
```

**New user?** Follow the [step-by-step Quick start](plugins/fulcrum/README.md#quick-start-step-by-step): it takes about 10 minutes and covers AWS sign-in, permissions, network access and settings. AWS admins: see [grant access](plugins/fulcrum/README.md#for-aws-administrators-grant-access).

See the [plugin README](plugins/fulcrum/README.md) for prerequisites (uv, AWS credentials, network access to MSK), the least-privilege IAM policy, configuration, every command with sample output, troubleshooting and security notes.

## Repository layout

```
.claude-plugin/marketplace.json   marketplace catalog (lists the fulcrum plugin)
plugins/fulcrum/                  the plugin (manifest, skills, MCP server, IAM policy, docs)
.github/workflows/ci.yml          lint + tests + plugin validation
```

## Releasing a new version

1. Bump `version` in `plugins/fulcrum/.claude-plugin/plugin.json` and `plugins/fulcrum/server/pyproject.toml` (and `__version__` in `src/fulcrum_mcp/__init__.py`), following semver.
2. Add an entry to `plugins/fulcrum/CHANGELOG.md`.
3. Merge to `main` and tag `fulcrum-vX.Y.Z`. Installed users get the update when the marketplace refreshes (`/plugin marketplace update fulcrum-marketplace`).

## License

MIT
