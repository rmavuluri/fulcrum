"""Optional CloudWatch metrics for MSK (``AWS/Kafka`` namespace)."""

from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime, timedelta
from typing import Any

from .errors import translate_aws_error
from .models import TopicThroughput

NAMESPACE = "AWS/Kafka"
_MAX_QUERIES = 500  # GetMetricData limit per request
_METRICS = {"BytesInPerSec": "bytes_in_per_sec", "MessagesInPerSec": "messages_in_per_sec"}


class Metrics:
    def __init__(self, client: Any) -> None:
        self._client = client

    def topic_throughput(
        self,
        cluster_name: str,
        topics: list[str],
        broker_ids: list[int] | None,
        minutes: int = 15,
        now: datetime | None = None,
    ) -> dict[str, TopicThroughput]:
        """Average per-topic ingress over the last ``minutes``, summed across brokers.

        Provisioned clusters publish these metrics per (broker, topic); serverless clusters
        per topic only, which callers signal by passing ``broker_ids=None``.
        """
        end = now or datetime.now(UTC)
        start = end - timedelta(minutes=minutes)
        period = 60

        queries: list[dict[str, Any]] = []
        lookup: dict[str, tuple[str, str]] = {}
        for topic in topics:
            for metric in _METRICS:
                for broker in broker_ids if broker_ids is not None else [None]:
                    qid = f"q{len(queries)}"
                    dims = [{"Name": "Cluster Name", "Value": cluster_name}, {"Name": "Topic", "Value": topic}]
                    if broker is not None:
                        dims.insert(1, {"Name": "Broker ID", "Value": str(broker)})
                    queries.append(
                        {
                            "Id": qid,
                            "MetricStat": {
                                "Metric": {"Namespace": NAMESPACE, "MetricName": metric, "Dimensions": dims},
                                "Period": period,
                                "Stat": "Average",
                            },
                            "ReturnData": True,
                        }
                    )
                    lookup[qid] = (topic, metric)

        # A series can be split across NextToken pages, so gather values per query first.
        series: dict[str, list[float]] = defaultdict(list)
        for chunk_start in range(0, len(queries), _MAX_QUERIES):
            chunk = queries[chunk_start : chunk_start + _MAX_QUERIES]
            for result in self._fetch(chunk, start, end):
                series[result["Id"]].extend(result.get("Values") or [])

        totals: dict[str, dict[str, float]] = defaultdict(dict)
        for qid, values in series.items():
            if not values:
                continue
            topic, metric = lookup[qid]
            totals[topic][metric] = totals[topic].get(metric, 0.0) + sum(values) / len(values)

        return {
            topic: TopicThroughput(
                topic=topic,
                bytes_in_per_sec=totals.get(topic, {}).get("BytesInPerSec"),
                messages_in_per_sec=totals.get(topic, {}).get("MessagesInPerSec"),
            )
            for topic in topics
        }

    def _fetch(self, queries: list[dict[str, Any]], start: datetime, end: datetime) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        kwargs: dict[str, Any] = {"MetricDataQueries": queries, "StartTime": start, "EndTime": end}
        try:
            while True:
                resp = self._client.get_metric_data(**kwargs)
                results.extend(resp.get("MetricDataResults", []))
                token = resp.get("NextToken")
                if not token:
                    break
                kwargs["NextToken"] = token
        except Exception as exc:
            raise translate_aws_error(exc, "cloudwatch:GetMetricData") from exc
        return results
