# Native streaming and recovery experiment

**Date:** September 25, 2026. **Result:** Passed correctness and recovery checks.
**Scope:** Real Kafka 3.9.1, Spark 3.5.6, Iceberg 1.8.1, and the project's Python processor,
using SQLite as the serving database. Airflow 2.10.5 DAG task was tested separately.
Docker and PostgreSQL were unverified in this native experiment; subsequent container
acceptance passed on September 30 (see [Validation](VALIDATION.md)). Cloud deployment
and live Airflow scheduling remain unverified.

## Measured burst after warmup

| Measurement | Observed result |
|---|---:|
| Synthetic input records | 941 |
| Unique valid events | 870 |
| Distinct entities | 300 |
| Time from first send until output drained | 11.67 seconds |
| Unique valid events / drain time | 74.54 per second |
| P50 publish-to-observed-commit latency | 8.33 seconds |
| P95 latency | 10.71 seconds |
| P99 latency | 10.83 seconds |

These are one local synthetic burst's measurements, **not sustained capacity**, not
bank-scale throughput, and not PostgreSQL performance. Spark used `local[2]`, 1 GB
driver memory, a 5-second trigger, and a 500-offset-per-trigger bound. First-start
warmup is excluded. Producer-to-database latency starts before Kafka send and ends
when a separate observer first sees the committed event. Polling interval was 100 ms;
query/observer scheduling can add delay. The dashboard's refresh delay is not included.
The suggested P95 <10-second target was **not met** in this run.

## Recovery and idempotency

The processor was forcibly stopped between workloads. While it was down, 152 records
for 50 additional entities were published. Restart used the same Kafka data, Iceberg
warehouse and Spark checkpoint. All backlog records drained **11.70 seconds after
restart**. This demonstrates process restart/backlog recovery, not a fault injected
at a precisely controlled point inside a SQL or Iceberg commit.

Twenty accepted events were then redelivered at new Kafka offsets with identical
payloads. All 370 entities still reconciled. Final counts:

| Layer | Result |
|---|---:|
| SQL raw deliveries | 1,177 |
| Accepted unique events | 1,068 |
| Duplicate deliveries | 74 |
| Quarantined malformed records | 35 |
| Current payment/claim entities | 370 |
| Iceberg bronze raw records | 1,177; unique source-position keys |
| Iceberg silver entities | 370 |
| Iceberg gold aggregate rows | 6; aggregate entity count 370 |

Warmup contributed 20 entities, measured burst 300, recovery 50. Reconciliation
matched the entire population at every stage. The Airflow DAG task also matched all
370 against the same reference snapshot. `airflow dags test` executes the DAG task;
it does not demonstrate a continuously running scheduler or Airflow container.

## Bug discovered and fixed

The first native streaming attempt failed because the foreachBatch DataFrame's
temporary view was not visible to the outer SparkSession. The merge now runs on
`batch.sparkSession`. The final native run and 22 Python tests passed after this fix.
Another setup attempt hit Java's unconfigured network proxy; downloading the exact
Maven connector artifacts through the working HTTP client resolved dependency access.
No network restrictions or credentials were changed.

## Reproduce

Use Linux (tested) or a compatible Unix environment; on Windows use WSL 2. Install
Java 17, download/extract Apache Kafka 3.9.1, and install the project extras:

```bash
python -m pip install -e ".[dev,spark]"
python scripts/validate_native.py --kafka-home /path/to/kafka_2.13-3.9.1 --work-dir /tmp/clearflow-new-run --count 300
```

The work directory must not already exist. The runner starts isolated Kafka and Spark
processes, performs the experiment, writes `results.json` and logs, and stops its own
processes. Default ports: 19094 and 19095. Change `--port` if needed. If Java cannot
resolve Maven dependencies, use `--jars-dir` with these exact connector artifacts:

- spark-sql-kafka-0-10_2.12:3.5.6
- spark-token-provider-kafka-0-10_2.12:3.5.6
- iceberg-spark-runtime-3.5_2.12:1.8.1
- kafka-clients:3.4.1
- commons-pool2:2.11.1

Hashes of the jars used here are in `evidence/connector-sha256.json`. Evidence includes
the original native runner output, result JSON, runtime description and Airflow success
log. Preserve these limitations when describing the results on GitHub or in interviews.

## Dashboard walkthrough

`demo/clearflow-dashboard-walkthrough.mp4` presents four actual browser views of the
verified dataset. The source is paused. It is a screenshot sequence, not a live-arrival
recording. Browser inspection found and fixed theme contrast and a chart-refresh error;
all four views then rendered with zero page errors, and the dashboard application test
passed again. Plotly 6.3.0 is now pinned for stable chart updates.
