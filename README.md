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
plugins/fulcrum/                  the plugin (manifest, skills, .mcp.json, IAM policy, docs)
packages/fulcrum-mcp/             the MCP server, published to PyPI as fulcrum-mcp
pyproject.toml, uv.lock, tests/   dev environment and tests (not shipped with the plugin)
.github/workflows/ci.yml          lint + tests + plugin validation
.github/workflows/publish.yml     builds, tests and publishes fulcrum-mcp to PyPI
```

## Releasing a new version

The server (`fulcrum-mcp` on PyPI) is released first, then the plugin is pointed at it. A PyPI
version can never be re-uploaded, even after deleting it, so check everything before step 2.

1. **Server.** Bump `version` in `packages/fulcrum-mcp/pyproject.toml` and `__version__` in
   `packages/fulcrum-mcp/src/fulcrum_mcp/__init__.py`. Merge to `main` with CI green.
2. **Publish.** Tag that commit `fulcrum-mcp-vX.Y.Z` and push the tag. The `Publish to PyPI`
   workflow builds the package, starts the built server and lists its tools, then uploads it
   with trusted publishing (no token; approve the `pypi` environment if prompted).
3. **Plugin.** Set `fulcrum-mcp==X.Y.Z` in `plugins/fulcrum/.mcp.json` and
   `plugins/fulcrum/pyproject.toml`, bump `version` in `plugins/fulcrum/.claude-plugin/plugin.json`
   and `plugins/fulcrum/pyproject.toml`, run `uv lock --project plugins/fulcrum`, and add an
   entry to `plugins/fulcrum/CHANGELOG.md`.
4. Merge to `main` and tag `fulcrum-vX.Y.Z`. Installed users get the update when the marketplace
   refreshes (`/plugin marketplace update fulcrum-marketplace`).

## License

MIT
