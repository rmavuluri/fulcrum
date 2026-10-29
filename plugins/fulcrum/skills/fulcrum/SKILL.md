---
name: fulcrum
description: Query Amazon MSK (Kafka) and AWS Glue Schema Registry — topics, consumer groups, lag, consumers, producers, schemas, schema diffs, cluster info and health. Read-only.
argument-hint: "<topics|topic|consumer-groups|consumer-group|consumers|producers|schemas|schema|schema-diff|cluster|clusters|health|status|help> [args] [key=value ...]"
disable-model-invocation: true
allowed-tools:
  - mcp__plugin_fulcrum_msk__fulcrum_status
  - mcp__plugin_fulcrum_msk__list_clusters
  - mcp__plugin_fulcrum_msk__get_cluster
  - mcp__plugin_fulcrum_msk__list_topics
  - mcp__plugin_fulcrum_msk__describe_topic
  - mcp__plugin_fulcrum_msk__list_consumer_groups
  - mcp__plugin_fulcrum_msk__describe_consumer_group
  - mcp__plugin_fulcrum_msk__list_consumers
  - mcp__plugin_fulcrum_msk__get_producer_activity
  - mcp__plugin_fulcrum_msk__cluster_health
  - mcp__plugin_fulcrum_msk__list_schemas
  - mcp__plugin_fulcrum_msk__get_schema
  - mcp__plugin_fulcrum_msk__diff_schema_versions
---

# /fulcrum

The user ran `/fulcrum` with these arguments:

```
$ARGUMENTS
```

Route them to exactly one Fulcrum MCP tool, then show the result.

## 1. Parse the arguments

- Split on whitespace, keeping quoted strings together. The first token is the **subcommand** (case-insensitive). The other tokens are either **positional** arguments or **`key=value` options**.
- If there are no arguments, or the subcommand is `help`, `-h` or `--help`, print the **Help** block below exactly and stop. Don't call any tool.
- If the subcommand isn't in the table, say `Unknown subcommand "<x>".`, print the Help block and stop.
- If a required positional argument is missing, print that row's usage line plus one example, and stop.

## 2. Call the tool

All tools belong to the `msk` server of this plugin (`mcp__plugin_fulcrum_msk__<tool>`).

| Subcommand | Positional args | Tool | Parameters (pass only the ones the user gave) |
|---|---|---|---|
| `topics` | — | `list_topics` | `prefix`, `include_internal` (from `internal=true`), `limit`, `offset`, `cluster_arn` (from `cluster=`) |
| `topic` | `<name>` | `describe_topic` | `name`, `cluster_arn` |
| `consumer-groups` (alias `groups`) | — | `list_consumer_groups` | `prefix`, `state`, `limit`, `offset`, `cluster_arn` |
| `consumer-group` (alias `group`) | `<id>` | `describe_consumer_group` | `group_id`, `cluster_arn` |
| `consumers` | — | `list_consumers` | `group_prefix` (from `prefix=` or `group=`), `limit`, `offset`, `cluster_arn` |
| `producers` | — | `get_producer_activity` | `topic_prefix` (from `prefix=`), `minutes`, `cluster_arn` |
| `schemas` | — | `list_schemas` | `registry`, `prefix`, `limit`, `offset` |
| `schema` | `<name> [version]` | `get_schema` | `name`, `version` (an integer; omit for latest), `registry` |
| `schema-diff` | `<name> <v1> <v2>` | `diff_schema_versions` | `name`, `v1`, `v2` (integers), `registry` |
| `cluster` | — | `get_cluster` | `cluster_arn` |
| `clusters` | — | `list_clusters` | — |
| `health` | — | `cluster_health` | `lag_threshold` (from `threshold=` or `lag=`), `cluster_arn` |
| `status` | — | `fulcrum_status` | — |

Option rules:

- Convert numeric options (`limit`, `offset`, `minutes`, `threshold`, versions) to integers. A leading `v` on a version is allowed (`v3` → `3`).
- `internal=true|yes|1` → `include_internal: true`.
- Ignore option keys that aren't listed for the subcommand, and mention that you ignored them in one short line.
- Never invent values. Don't guess a topic, group or schema name the user didn't type.

## 3. Show the result

- On success, output the tool's markdown **as-is**: tables, counts and the "Fetched at" line. Don't re-summarize it or turn it into prose.
- After the output you may add **one** short line if something needs attention (for example offline partitions, or a group with very high lag). Otherwise add nothing.
- If the result says rows were omitted, keep the tool's paging hint (`offset=…`, `prefix=…`).
- On error, show the error text the tool returned, including any hint. Then add: `Run /fulcrum status to check configuration, credentials and connectivity.` If the error lists clusters to choose from, ask the user which one to use, then re-run with `cluster=<arn>`.
- Fulcrum is read-only. If the user asks this command to change anything (reset offsets, delete a topic, register a schema), say Fulcrum can't make changes.

## Help

Print this block exactly when help is requested:

```
Fulcrum: read-only Amazon MSK (Kafka) and AWS Glue Schema Registry

Usage: /fulcrum <subcommand> [args] [key=value ...]

Kafka
  topics                          List topics (name, partitions, replication, internal)
                                    prefix=<p> internal=true limit=<n> offset=<n>
  topic <name>                    Partitions, leaders, ISR, offsets, configs
  consumer-groups                 Groups with state and member count
                                    prefix=<p> state=<STABLE|EMPTY|...> limit=<n> offset=<n>
  consumer-group <id>             Members, assignments, committed offsets, lag
  consumers                       Active consumer clients across all groups   prefix=<group-prefix>
  producers                       Producer activity (best available)           prefix=<p> minutes=<n>
  health                          Under-replicated/offline partitions, lagging groups   threshold=<n>

Cluster
  cluster                         ARN, Kafka version, brokers, endpoints, auth, state
  clusters                        List MSK clusters in the region
  status                          Check config, credentials and broker connectivity

Glue Schema Registry
  schemas                         Registries and schemas      registry=<r> prefix=<p>
  schema <name> [version]         Definition, format, compatibility, versions   registry=<r>
  schema-diff <name> <v1> <v2>    Field changes and unified diff between versions

Global options
  cluster=<arn>                   Query a different MSK cluster for one command

Examples
  /fulcrum topics prefix=orders
  /fulcrum consumer-group payments-service
  /fulcrum schema-diff OrderCreated 3 4
  /fulcrum health threshold=5000

You can also just ask, e.g. "which consumer group is lagging the most?"
```
