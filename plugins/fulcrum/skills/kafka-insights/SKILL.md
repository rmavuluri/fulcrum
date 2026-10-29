---
name: kafka-insights
description: Answer questions about Amazon MSK / Kafka clusters and AWS Glue schemas with the Fulcrum MCP tools. Use when the user asks about Kafka topics, partitions, consumer groups, consumer lag, consumers, producers or throughput, broker or partition health, MSK cluster details, or Glue Schema Registry schemas, versions, compatibility or schema changes.
when_to_use: 'Examples: "which consumer group is lagging?", "is the orders topic under-replicated?", "who is consuming payments?", "what changed in the OrderCreated schema?", "how many partitions does clickstream have?", "is my Kafka cluster healthy?"'
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

# Kafka insights with Fulcrum

Fulcrum's MCP server (`mcp__plugin_fulcrum_msk__*`) has read-only access to Amazon MSK and the AWS Glue Schema Registry. Every tool returns markdown with counts and a "Fetched at" timestamp.

## Pick the tool for the question

| Question | Tool(s) |
|---|---|
| Is anything wrong? Is the cluster healthy? | `cluster_health` |
| Which consumer group is lagging, or lagging the most? | `cluster_health` (groups are sorted by lag). Pass `lag_threshold: 0` to see every group with any lag. Then `describe_consumer_group` on the worst group if the user wants detail |
| Why is group X behind? Which partitions? | `describe_consumer_group` (per-partition lag, owners, members) |
| Who consumes topic T? | `list_consumers`, then keep the rows whose assignments include T. Committed-but-idle groups need `list_consumer_groups` plus `describe_consumer_group` |
| Who produces to topic T? | `get_producer_activity` with `topic_prefix`. Say plainly that Kafka can't list producers |
| Topic details, partitions, retention, ISR | `describe_topic` |
| Find topics | `list_topics` with `prefix` |
| Cluster version, brokers, endpoints, auth | `get_cluster` |
| Schema definition, compatibility, versions | `get_schema` |
| What changed between schema versions? | `get_schema` (to learn the version numbers), then `diff_schema_versions` |
| Anything fails with config, credential or network errors | `fulcrum_status` |

## How to answer

- Lead with the direct answer in one or two sentences ("`payments-audit` is furthest behind: 4,000 messages, all on `payments[0]`"). Then include the relevant part of the tool's table. Keep the "Fetched at" line.
- Make the smallest number of calls that answers the question. Don't call every tool.
- Lag is counted in messages (end offset − committed offset), not time. A group in state `EMPTY` that has lag usually means its consumers are down. Lag that grows across repeated checks means the consumers can't keep up.
- Under-replicated partitions (ISR smaller than the replica set) reduce durability. Offline partitions (no leader) are unavailable: call these out first.
- For large clusters, use `prefix` filters rather than paging through everything.

## Limits to be honest about

- **Producers:** Kafka has no API that lists producer clients. The best evidence is per-topic ingress from CloudWatch. Never present it as a producer list.
- **Topic size on disk:** the Kafka AdminClient doesn't expose it. Fulcrum reports approximate retained message counts instead.
- **Network:** MSK brokers are usually in a private VPC. If a tool reports "Network unreachable", relay its hint (VPN, bastion, or running inside the VPC). Don't retry in a loop.
- **Access denied:** the error names the missing IAM action. Point the user to `iam/fulcrum-readonly-policy.json` in the plugin.
- **Read-only:** Fulcrum can't reset offsets, create or delete topics, change configs, or register schemas. If asked, say so. You may suggest the AWS or Kafka CLI command the user could run themselves, clearly labelled as something they run.

## Multiple clusters

If a tool returns a list of clusters and asks which to use, ask the user to pick. Then pass `cluster_arn` on later calls in this conversation.
