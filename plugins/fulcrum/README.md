# Fulcrum

Query **Amazon MSK (Kafka)** and the **AWS Glue Schema Registry** from inside Claude Code, using IAM auth. You can list topics, check consumer-group lag, see who is consuming, inspect and diff schemas, and run a cluster health check, either with `/fulcrum` subcommands or by asking in plain English ("which consumer group is lagging?").

Fulcrum is **read-only**. No tool can create, alter or delete topics, groups, offsets, configs or schemas.

- [How it works](#how-it-works)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [IAM policy](#iam-policy)
- [Commands](#commands) (with sample output)
- [Limitations](#limitations)
- [Troubleshooting](#troubleshooting)
- [Security notes](#security-notes)
- [Development](#development)

## How it works

```
/fulcrum <subcommand>  ──►  skill "fulcrum" (routes $ARGUMENTS)  ──┐
"which group is lagging?" ─► skill "kafka-insights"  ──────────────┤
                                                                    ▼
                         local MCP server "msk" (Python, uv) ── read-only tools
                           ├─ boto3 kafka       → cluster metadata, bootstrap brokers
                           ├─ confluent-kafka   → one long-lived AdminClient (SASL_SSL + OAUTHBEARER/IAM)
                           ├─ boto3 glue        → registries, schemas, versions
                           └─ boto3 cloudwatch  → per-topic ingress (producer activity)
```

- The MCP server runs on your machine and uses your AWS credentials from the standard credential chain.
- Kafka calls go through a single `AdminClient` that is reused for the whole session. Its IAM tokens come from `aws-msk-iam-sasl-signer-python` and refresh automatically before they expire.
- Calls are batched: one metadata request for all topics, one describe for all groups, and a single end-offset lookup for lag. Results are cached for a short time (30 s by default).

## Prerequisites

| Requirement | Details |
|---|---|
| Claude Code | v2.1.281 or later. `claude --version` |
| uv | [Install uv](https://docs.astral.sh/uv/getting-started/installation/). It downloads Python 3.12 and the pinned dependencies on first run, so you don't need a system Python |
| AWS credentials | Any standard source: `AWS_PROFILE` (including SSO), environment variables, or an instance/task role |
| IAM permissions | See [IAM policy](#iam-policy) |
| Network access to the brokers | MSK brokers usually sit in a private VPC. The machine running Claude Code needs a path to them: VPN, SSH tunnel through a bastion, or Claude Code running inside the VPC (EC2, Cloud9, a dev container). The brokers' security group must allow TCP **9098** (IAM, private) or **9198** (IAM, public access) from you. The AWS API calls (MSK control plane, Glue, CloudWatch) only need normal internet access |
| Cluster auth | The cluster must have **IAM role-based authentication** enabled. Provisioned and Serverless clusters are both supported |

## Installation

From inside Claude Code:

```
/plugin marketplace add rmavuluri/fulcrum
/plugin install fulcrum@fulcrum-marketplace
```

Or from your shell:

```bash
claude plugin marketplace add rmavuluri/fulcrum
claude plugin install fulcrum@fulcrum-marketplace
```

When you install from inside a session, Claude Code shows the [configuration](#configuration) dialog. Every field is optional. Restart Claude Code (or run `/reload-plugins`), then check the install:

```
/fulcrum help
/fulcrum status
```

The first start creates the Python environment in `~/.claude/plugins/data/fulcrum-fulcrum-marketplace/venv`, which takes about 10–30 s. Later starts take about a second. If `/mcp` shows the server timing out on the very first start, run `/mcp` → reconnect, or start Claude Code with `MCP_TIMEOUT=60000`.

## Configuration

Set values in the plugin's options (`/plugin` → Installed → fulcrum → Configure, or `/config`). If an option is empty, Fulcrum falls back to the environment variable. Restart Claude Code after changing either.

| Option | Env var fallback | Default | Meaning |
|---|---|---|---|
| `aws_region` | `AWS_REGION`, `AWS_DEFAULT_REGION` | region in `MSK_CLUSTER_ARN` | Region of the cluster and registry |
| `aws_profile` | `AWS_PROFILE` | default chain | Named profile from `~/.aws/config` |
| `msk_cluster_arn` | `MSK_CLUSTER_ARN` | auto | Cluster to query. If unset and the region has exactly one cluster, Fulcrum uses it. Otherwise it lists the clusters and asks you to pick |
| `glue_registry_name` | `GLUE_REGISTRY_NAME` | all registries | Limit schema commands to one registry |
| `lag_threshold` | `FULCRUM_LAG_THRESHOLD` | `1000` | Total lag above which `health` flags a group |
| `cache_ttl` | `FULCRUM_CACHE_TTL` | `30` | Seconds to reuse results. `0` disables the cache |
| — | `FULCRUM_TIMEOUT` | `15` | Per-request timeout in seconds (AWS and Kafka) |
| — | `FULCRUM_MAX_ROWS` | `100` | Rows per page before output is summarized and paged |
| — | `FULCRUM_LOG_LEVEL` | `INFO` | Server log level. Logs go to stderr as JSON |

To query a different cluster for one command, add `cluster=<arn>`, e.g. `/fulcrum topics cluster=arn:aws:kafka:...`.

## IAM policy

[`iam/fulcrum-readonly-policy.json`](iam/fulcrum-readonly-policy.json) contains only the actions Fulcrum actually calls. Replace `REGION`, `ACCOUNT_ID` and `CLUSTER_NAME`. To restrict schema access, replace the `registry/*` and `schema/*/*` wildcards with specific registry names.

| Action | Why Fulcrum needs it | Used by |
|---|---|---|
| `kafka:ListClustersV2` | List clusters so you can pick one (it doesn't support resource scoping, hence `*`) | `clusters`, auto-select |
| `kafka:DescribeClusterV2` | Cluster ARN, version, broker count and type, auth types, state | `cluster`, `health`, `producers` |
| `kafka:GetBootstrapBrokers` | Find the SASL/IAM bootstrap endpoint to connect to | every Kafka command |
| `kafka-cluster:Connect` | Open an IAM-authenticated connection to the brokers | every Kafka command |
| `kafka-cluster:DescribeCluster` | Read broker and controller metadata | `topics`, `health`, `status` |
| `kafka-cluster:DescribeTopic` | Topic metadata, partitions, leaders, ISR, earliest/latest offsets | `topics`, `topic`, lag, `health` |
| `kafka-cluster:DescribeTopicDynamicConfiguration` | Topic config overrides (retention, cleanup policy, …) | `topic` |
| `kafka-cluster:DescribeGroup` | Consumer groups, members, assignments, committed offsets | `consumer-groups`, `consumer-group`, `consumers`, `health` |
| `glue:ListRegistries` | List registries (it doesn't support resource scoping, hence `*`) | `schemas`, schema lookup |
| `glue:ListSchemas` | Schemas in a registry | `schemas`, schema lookup |
| `glue:GetSchema` | Format, compatibility mode, latest version | `schema` |
| `glue:ListSchemaVersions` | Version history | `schema` |
| `glue:GetSchemaVersion` | Schema definitions | `schema`, `schema-diff` |
| `cloudwatch:GetMetricData` | Per-topic `BytesInPerSec` / `MessagesInPerSec` (it doesn't support resource scoping) | `producers` |

The policy leaves out two actions on purpose:

- `glue:GetRegistry`: Fulcrum gets everything it needs from `ListRegistries`.
- `kafka-cluster:DescribeTransactionalId`: the current confluent-kafka Python client doesn't expose transaction APIs (see [Limitations](#limitations)).

`sts:GetCallerIdentity`, which `status` uses to show which identity you're running as, needs no permission.

## Commands

The full command is `/fulcrum:fulcrum <subcommand>`. Claude Code also accepts the short form `/fulcrum <subcommand>` unless another command already uses that name. With no arguments, `/fulcrum` shows help.

Every listing is sorted and shows a count and a `Fetched at` timestamp (marked `(cached)` when it came from the cache). Listings longer than 100 rows are paged; use `limit=`, `offset=` or `prefix=`.

| Command | What it shows | Options |
|---|---|---|
| `/fulcrum topics` | Topics: partitions, replication factor, internal flag | `prefix=` `internal=true` `limit=` `offset=` |
| `/fulcrum topic <name>` | Partitions, leaders, replicas, ISR, offsets, approximate message count, config overrides | |
| `/fulcrum consumer-groups` | Groups with state and member count | `prefix=` `state=` `limit=` `offset=` |
| `/fulcrum consumer-group <id>` | Members, assignments, committed offsets, per-partition and total lag | |
| `/fulcrum consumers` | Active consumer clients across all groups | `prefix=` (group prefix) |
| `/fulcrum producers` | Best available producer information (see [Limitations](#limitations)) | `prefix=` `minutes=` |
| `/fulcrum schemas` | Glue registries and schemas | `registry=` `prefix=` |
| `/fulcrum schema <name> [version]` | Definition, format, compatibility, version history | `registry=` |
| `/fulcrum schema-diff <name> <v1> <v2>` | Field-level changes plus a unified diff | `registry=` |
| `/fulcrum cluster` | ARN, Kafka version, brokers, broker type, endpoints, auth, state | |
| `/fulcrum health` | Cluster state, under-replicated and offline partitions, groups over the lag threshold | `threshold=` |
| `/fulcrum clusters` | MSK clusters in the region | |
| `/fulcrum status` | Resolved config plus credential and connectivity checks | |
| `/fulcrum help` | Usage | |

All Kafka and cluster commands also accept `cluster=<arn>`.

### Sample output

`/fulcrum topics`

```markdown
## Topics

**3 topics** (1 internal hidden; add `internal=true` to show), 4 partitions total.

| Topic | Partitions | Replication | Internal |
|---|---|---|---|
| broken | 1 | 2 | no |
| orders | 2 | 3 | no |
| payments | 1 | 3 | no |

_Fetched at 2026-10-28 18:42:07 UTC_
```

`/fulcrum topic orders`

```markdown
## Topic `orders`

| Field | Value |
|---|---|
| Partitions | 2 |
| Replication factor | 3 |
| Internal | no |
| Messages retained (approx.) | 150 |
| Under-replicated partitions | 1 |
| Offline partitions | 0 |
| Size on disk | n/a (not exposed by Kafka AdminClient) |

### Partitions

| Partition | Leader | Replicas | ISR | Earliest | Latest |
|---|---|---|---|---|---|
| 0 | 1 | 1,2,3 | 1,2,3 | 10 | 110 |
| 1 | 2 | 2,3,1 | 2,3 ⚠️ | 0 | 50 |

### Configuration (overrides and key settings)

| Config | Value |
|---|---|
| cleanup.policy | delete |
| min.insync.replicas | 2 |
| retention.ms | 604800000 |

_Fetched at 2026-10-28 18:42:07 UTC_
```

`/fulcrum consumer-groups`

```markdown
## Consumer groups

**2 consumer groups** — empty: 1, stable: 1

| Group | State | Members | Type |
|---|---|---|---|
| orders-consumer | STABLE | 1 | CLASSIC |
| payments-audit | EMPTY | 0 | CLASSIC |

_Fetched at 2026-10-28 18:42:07 UTC_
```

`/fulcrum consumer-group orders-consumer`

```markdown
## Consumer group `orders-consumer`

| Field | Value |
|---|---|
| State | STABLE |
| Type | CLASSIC |
| Assignor | range |
| Coordinator broker | 1 |
| Members | 1 |
| Partitions with commits | 2 |
| Partitions lagging | 1 |
| **Total lag** | 10 |

### Members

| Client id | Host | Member id | Assigned partitions |
|---|---|---|---|
| orders-service | 10.0.1.5 | m-1 | orders[0,1] |

### Partition lag

| Topic | Partition | Committed | End offset | Lag | Owner |
|---|---|---|---|---|---|
| orders | 0 | 100 | 110 | 10 | orders-service |
| orders | 1 | 50 | 50 | 0 | orders-service |

_Fetched at 2026-10-28 18:42:07 UTC_
```

`/fulcrum consumers`

```markdown
## Active consumers

**1 active consumer clients** across 1 groups on 1 hosts.

| Client id | Host | Group | Assignments |
|---|---|---|---|
| orders-service | 10.0.1.5 | orders-consumer | orders[0,1] |

_Fetched at 2026-10-28 18:42:07 UTC_
```

`/fulcrum producers`

```markdown
## Producer activity (best available)

> **Kafka has no API that lists producers.** Producers are not registered with the cluster the way
> consumer groups are, so a complete producer list (client ids, hosts) is not available. Below is the
> best available evidence: per-topic ingress from CloudWatch (`AWS/Kafka` `BytesInPerSec` / `MessagesInPerSec`).

**2 topics received writes** in the last 15 min; 1 topics idle or without data.

| Topic | Messages in/s (avg) | Bytes in/s (avg) |
|---|---|---|
| payments | 41.20 | 38.6 KiB |
| orders | 4.00 | 2.0 KiB |

_Transactional producer ids: not available — the installed confluent-kafka AdminClient does not expose ListTransactions/DescribeProducers._

_Fetched at 2026-10-28 18:42:07 UTC_
```

`/fulcrum health`

```markdown
## Health of `orders-prod`

⚠️ **3 issue type(s) found**

| Field | Value |
|---|---|
| Cluster state | ACTIVE |
| Partitions checked | 5 |
| Under-replicated partitions | 1 |
| Offline partitions | 1 |
| Groups checked | 2 |
| Groups with lag > 1,000 | 1 |

### Offline partitions

| Topic | Partition | Replicas |
|---|---|---|
| broken | 0 | 1,2 |

### Under-replicated partitions

| Topic | Partition | Leader | Replicas | ISR |
|---|---|---|---|---|
| orders | 1 | 2 | 2,3,1 | 2,3 |

### Consumer groups with lag > 1,000

| Group | State | Total lag | Max partition lag | Partitions lagging |
|---|---|---|---|---|
| payments-audit | EMPTY | 4,000 | 4,000 | 1 |

_Fetched at 2026-10-28 18:42:07 UTC_
```

`/fulcrum cluster`

```markdown
## Cluster `orders-prod`

| Field | Value |
|---|---|
| Name | orders-prod |
| ARN | arn:aws:kafka:us-east-1:123456789012:cluster/orders-prod/11111111-2222-3333-4444-555555555555-1 |
| Type | PROVISIONED |
| State | ACTIVE |
| Kafka version | 3.6.0 |
| Broker count | 3 |
| Broker type | kafka.m7g.large |
| Client auth | SASL/IAM |
| Encryption in transit | TLS |
| Enhanced monitoring | DEFAULT |

### Bootstrap endpoints

| Endpoint | Bootstrap brokers |
|---|---|
| SASL/IAM (private) | b-1.orders.kafka.us-east-1.amazonaws.com:9098,b-2.orders.kafka.us-east-1.amazonaws.com:9098 |
| TLS (private) | b-1.orders.kafka.us-east-1.amazonaws.com:9094 |

_Fetched at 2026-10-28 18:42:07 UTC_
```

`/fulcrum schemas`

```markdown
## Glue Schema Registry

**1 registries**, **2 schemas**.

### Registries

| Registry | Status | Schemas | Description |
|---|---|---|---|
| prod | AVAILABLE | 2 | Production event schemas |

### Schemas

| Registry | Schema | Status | Updated |
|---|---|---|---|
| prod | OrderCreated | AVAILABLE | 2026-10-20T09:14:02+00:00 |
| prod | PaymentSettled | AVAILABLE | 2026-09-02T16:40:51+00:00 |

Run `/fulcrum schema <name>` for format, compatibility and versions.

_Fetched at 2026-10-28 18:42:07 UTC_
```

`/fulcrum schema OrderCreated`

````markdown
## Schema `OrderCreated` v2

| Field | Value |
|---|---|
| Registry | prod |
| Format | AVRO |
| Compatibility | BACKWARD |
| Status | AVAILABLE |
| Latest version | 2 |
| Showing version | 2 |

### Definition

```json
{
  "type": "record",
  "name": "OrderCreated",
  "fields": [
    { "name": "id", "type": "string" },
    { "name": "amount", "type": "long" },
    { "name": "currency", "type": "string", "default": "USD" }
  ]
}
```

### Versions (2)

| Version | Status | Created |
|---|---|---|
| 2 | AVAILABLE | 2026-10-20T09:14:02+00:00 |
| 1 | AVAILABLE | 2026-06-11T12:00:31+00:00 |

_Fetched at 2026-10-28 18:42:07 UTC_
````

`/fulcrum schema-diff OrderCreated 1 2`

````markdown
## Schema diff `OrderCreated` v1 → v2

| Field | Value |
|---|---|
| Registry | prod |
| Format | AVRO |
| Compared | v1 → v2 |

### Field changes

| Change | Field | Detail |
|---|---|---|
| added | currency | {"default": "USD", "type": "string"} |
| changed | amount | type: "int" → "long" |

### Unified diff

```diff
--- v1
+++ v2
@@ ...
-      "type": "int"
+      "type": "long"
...
```

_Fetched at 2026-10-28 18:42:07 UTC_
````

### Natural-language questions

The `kafka-insights` skill lets Claude use the same tools when you just ask:

- "Which consumer group is lagging the most, and on which partitions?"
- "Is anything under-replicated right now?"
- "Who consumes the `payments` topic?"
- "What changed between versions 3 and 4 of the OrderCreated schema?"

## Limitations

- **Producers:** Kafka has no API that lists producers, because producers don't register with the cluster the way consumer groups do. `producers` therefore reports per-topic ingress from CloudWatch (`BytesInPerSec`, `MessagesInPerSec`, averaged over the window and summed across brokers) and says so in its output. confluent-kafka 2.16 (the version pinned here) doesn't expose `DescribeProducers`/`ListTransactions`, so transactional producer ids aren't shown either. Fulcrum detects support at runtime and says when it's available.
- **Topic size on disk:** the confluent-kafka Python client doesn't expose `DescribeLogDirs`, and MSK doesn't publish per-topic disk usage. `topic` reports the approximate retained message count (latest − earliest offset per partition) instead.
- **Lag** is measured in messages (end offset − committed offset), not time. Partitions with no committed offset show `—`.
- **Committed offsets** are fetched one group per request, because librdkafka only accepts one group per call. All requests are issued concurrently, and end offsets are then fetched in a single batch.
- **Network:** see [Prerequisites](#prerequisites). Fulcrum can't reach brokers your machine can't reach.

## Troubleshooting

Start with `/fulcrum status`. It shows the resolved config and checks credentials, the cluster, and broker reachability, in that order.

| Symptom | Likely cause | Fix |
|---|---|---|
| `AWS credentials not found` | No credentials in the chain | `export AWS_PROFILE=<name>` (for SSO, also `aws sso login --profile <name>`), or set the `aws_profile` option. Restart Claude Code |
| `AWS rejected the credentials` / SSO token errors | Expired session | `aws sso login --profile <name>` |
| `Access denied … kafka-cluster:DescribeGroup` (or any action) | That IAM action is missing | Add the named action to your policy. See [IAM policy](#iam-policy) |
| `Network unreachable … Timed out waiting for the Kafka brokers` | No route to the private brokers | Connect your VPN or bastion tunnel, or run inside the VPC. Check the security group allows 9098/9198 from you. `nc -vz <broker-host> 9098` should connect |
| `No MSK cluster selected …` with a list | Several clusters in the region | Pass `cluster=<arn>`, or set `msk_cluster_arn` |
| `The cluster has no SASL/IAM bootstrap endpoint` | IAM auth isn't enabled on the cluster | Enable IAM role-based authentication on the cluster |
| `No AWS region configured` | Region not set | Set `aws_region` or `AWS_REGION` |
| `/fulcrum` not found | Another command already uses that name | Use `/fulcrum:fulcrum` |
| Server shows as failed in `/mcp` | `uv` not on PATH, or the first start timed out | Install uv. Retry with `MCP_TIMEOUT=60000 claude`. Check `claude --debug` output |
| `producers` shows no data | Metrics have no recent datapoints, or `cloudwatch:GetMetricData` is denied | Widen the window: `/fulcrum producers minutes=60`. Check the policy |

## Security notes

- **Read-only by construction.** The server only calls describe/list/get APIs. The Kafka wrapper has no method that could create, alter or delete anything. A test enforces that only read-only AdminClient methods are called, and every MCP tool is annotated `readOnlyHint: true`.
- **No secrets in the plugin.** Credentials come only from the standard AWS chain. None of the options are secret, and Fulcrum never accepts, stores or logs credentials. MSK IAM tokens live only in memory and are never logged. Log output is JSON on stderr, with signed-URL parameters redacted.
- **Input validation.** Topic, group, schema, registry, prefix and ARN inputs are checked against strict patterns before they reach any API.
- **Least privilege.** The bundled policy grants only the actions listed above, scoped to your cluster where AWS supports it.
- **Runs locally.** Data goes from AWS to your machine and into your Claude Code session. Treat topic names, hosts and schema definitions accordingly.
- **Pinned dependencies.** Every Python dependency is pinned in `server/uv.lock`, and the server runs with `uv run --frozen`.

## Development

```bash
# from the repository root
claude --plugin-dir ./plugins/fulcrum         # load the plugin without installing it
claude plugin validate ./plugins/fulcrum      # validate the plugin
claude plugin validate .                      # validate the marketplace

cd plugins/fulcrum/server
uv sync                                       # dev environment
uv run ruff check src tests && uv run ruff format --check src tests
uv run pytest                                 # unit tests, coverage gate 80%

# optional live test, against your real account and cluster
FULCRUM_INTEGRATION=1 AWS_PROFILE=<p> AWS_REGION=<r> MSK_CLUSTER_ARN=<arn> uv run pytest tests/test_integration.py --no-cov
```

Layout:

```
plugins/fulcrum/
├── .claude-plugin/plugin.json      manifest + userConfig
├── .mcp.json                       starts the "msk" server with uv
├── skills/fulcrum/SKILL.md         /fulcrum subcommand router
├── skills/kafka-insights/SKILL.md  natural-language guidance
├── iam/fulcrum-readonly-policy.json
└── server/                         Python MCP server (src/fulcrum_mcp, tests)
```

## License

[MIT](LICENSE)
