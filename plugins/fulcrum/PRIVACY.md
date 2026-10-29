# Fulcrum privacy policy

Last updated: 2026-10-29

Fulcrum is an open-source Claude Code plugin that answers questions about your own Amazon MSK
(Kafka) clusters and AWS Glue Schema Registry. It runs entirely on your machine. There is no
Fulcrum service, account or backend, and the author receives no data from you.

## What Fulcrum reads

Only metadata from the AWS account and Kafka cluster you point it at, and only when you ask a
question that needs it:

- MSK cluster details: name, state, Kafka version, broker count and endpoints.
- Kafka metadata: topic names and configuration, partitions, replicas, consumer group IDs and
  offsets, and the client ID and host address of each active consumer.
- CloudWatch metrics for the cluster (bytes and messages in, per topic).
- Glue Schema Registry registries, schema names, versions, definitions and compatibility.

Fulcrum never reads Kafka message contents. It uses the Kafka admin API only, never a consumer.
It does not read or collect names, email addresses or other personal data.

## Credentials

Fulcrum uses the standard AWS credential chain on your machine (a named profile, AWS SSO,
environment variables or an instance role), plus an optional IAM role that you configure it to
assume. It never asks for, stores, logs or transmits secret keys. Short-lived MSK IAM tokens stay
in memory and are never logged.

## Where data goes

- Requests go only to AWS (MSK, Kafka brokers, Glue, CloudWatch, STS) in your own account.
- Results are returned to your Claude Code session, where they are handled under your agreement
  with Anthropic.
- Fulcrum sends no analytics or telemetry and contacts no other service. Python dependencies are
  downloaded from PyPI once, when the plugin is installed or updated.

## Retention

Fulcrum stores nothing on disk. Results are cached in memory for a short time (30 seconds by
default, configurable with `cache_ttl`) and discarded when Claude Code exits. Diagnostic logs go
to stderr, which Claude Code manages; they contain no credentials or signed URLs.

## Contact

Questions or issues: https://github.com/rmavuluri/fulcrum/issues
