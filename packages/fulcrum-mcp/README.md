# fulcrum-mcp

The MCP server behind **[Fulcrum](https://github.com/rmavuluri/fulcrum/tree/main/plugins/fulcrum)**, a
Claude Code plugin for querying Amazon MSK (Kafka) and the AWS Glue Schema Registry with IAM auth:
topics, consumer groups and lag, active consumers, producer activity, schemas and schema diffs,
cluster info and health. Every tool is read-only.

Most people should install the plugin rather than this package; the plugin starts the server for you:

```
/plugin marketplace add rmavuluri/fulcrum
/plugin install fulcrum@fulcrum-marketplace
```

## Running the server directly

It is a stdio MCP server, so any MCP client can launch it:

```bash
uvx fulcrum-mcp
```

Configuration comes from environment variables (`AWS_PROFILE`, `AWS_REGION`, `MSK_CLUSTER_ARN` or
`MSK_BOOTSTRAP_BROKERS`, and more) and AWS credentials from the standard credential chain. See the
[plugin README](https://github.com/rmavuluri/fulcrum/blob/main/plugins/fulcrum/README.md) for the
full list, the least-privilege IAM policy and troubleshooting, and
[PRIVACY.md](https://github.com/rmavuluri/fulcrum/blob/main/plugins/fulcrum/PRIVACY.md) for what it
reads and where data goes.

## License

MIT
