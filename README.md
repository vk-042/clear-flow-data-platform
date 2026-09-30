# ClearFlow
### Real-time banking payments & healthcare claims intelligence

A runnable data engineering portfolio project using **Python, Kafka, Spark Structured
Streaming, Apache Iceberg, PostgreSQL, Airflow, SQL, Streamlit, and Isolation Forest**.
Two financial event lifecycles share reliable ingestion while enforcing separate
business rules. All data is synthetic. This is a demonstration, not a deployed bank,
clinical application, or certified compliance solution.

**Measured evidence:** [Streaming results](docs/NATIVE_RESULTS.md) · [Dashboard walkthrough](docs/demo/clearflow-dashboard-walkthrough.mp4)

**Start here:** [Architecture](docs/ARCHITECTURE.md) ·
[Windows setup](docs/QUICKSTART_WINDOWS.md) · [Runbook](docs/RUNBOOK.md) ·
[Data dictionary](docs/DATA_DICTIONARY.md) · [Validation](docs/VALIDATION.md)

## Features

- Separate three-partition Kafka topics for payments and claims.
- Idempotent Iceberg raw history keyed by Kafka topic/partition/offset.
- Strict versioned contracts; money stored as integer cents.
- Duplicate protection, conflict quarantine, and source-sequence state reconstruction.
- Late predecessor correction without silently dropping old events.
- Full-snapshot reconciliation against external-style CSV records.
- Separate synthetic Isolation Forest amount baselines for banking and healthcare.
- Live dashboard: current state, quality, anomalies, and reconciliation.
- Checkpoint recovery, JSONL replay, failure tests, and CI integration checks.

## Run without Docker

Python 3.10–3.12 required. Run from the extracted project folder:

```bash
python -m venv .venv
# Linux/macOS:
source .venv/bin/activate
# Windows PowerShell instead:
# .\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
python -m clearflow.cli demo --count 100
python -m streamlit run src/clearflow/dashboard.py
```

Open http://localhost:8501. Local mode uses SQLite and the same domain processor,
contracts, ML, and dashboard. It does **not** run Kafka, Spark, Iceberg, or Airflow.
Use a fresh database for each demo: repeated runs append entities while the reference
CSV covers only the newest run.

```bash
python -m clearflow.cli stats
python -m clearflow.cli replay data/events.jsonl
python -m clearflow.cli reconcile data/reference.csv
python -m clearflow.cli rebuild
python -m pytest -q
```

## Complete streaming stack

Docker Desktop/Engine with Compose v2 required. Start with 4 CPUs, 8 GB RAM and 8 GB
free disk; actual requirements depend on host and downloads. Internet access is
needed for images, pip packages, and Spark Maven connectors.

```bash
cp .env.example .env
# PowerShell: Copy-Item .env.example .env
docker compose config --quiet
docker compose up -d --build processor dashboard
docker compose --profile live up -d producer
docker compose logs -f processor
```

Open http://localhost:8501. First startup downloads connectors. Dashboard refreshes
every five seconds. The native burst measured **P95 10.71 seconds**, slightly missing the 10-second design target; see [measured results](docs/NATIVE_RESULTS.md).
The simulator uses historical timestamps to demonstrate source lateness.
One serving writer is supported: do not scale processor replicas.

### Finite acceptance test

Use a clean, separate Compose project. The last command deletes only its demo volumes.
Stop the regular stack first if its ports conflict with this acceptance project.

```bash
docker compose -p clearflow-acceptance up -d --build processor
docker compose -p clearflow-acceptance run --rm producer python -m clearflow.cli produce --count 20 --interval 0.02 --output /app/data/reference.csv
docker compose -p clearflow-acceptance run --rm dashboard python scripts/integration_check.py
docker compose -p clearflow-acceptance restart processor
docker compose -p clearflow-acceptance run --rm dashboard python scripts/integration_check.py
docker compose -p clearflow-acceptance down -v
```

### Airflow

```bash
docker compose --profile orchestration up -d --build airflow
docker compose logs airflow
```

Open http://localhost:8080; retrieve generated standalone credentials from startup
logs. The DAG starts paused. Before enabling, place a complete reference at
`/app/data/reference.csv` and wait for processing to catch up. The snapshot must cover
the same complete entity population as the database. This is not a settlement calendar.

## Domain semantics

| Banking | Healthcare |
|---|---|
| initiated → authorized → settled → reversed | submitted → accepted → paid → adjusted |
| initiated → declined | submitted → denied; accepted → denied |
| Authorization can also be reversed | An adjustment can change the current amount |

USD only. No partial settlements, split claims, FX, claim line items, chargeback
workflows, or payment execution. Reversals retain face amount and change status;
they are excluded from currently settled totals. Metrics count current entities,
not all status events. Source sequence is authoritative; timestamp regressions
and impossible transitions create visible issues.

## Layout

- `src/clearflow/`: contracts, simulator, processor, streaming, ML, dashboard, lake snapshots.
- `tests/`: correctness and failure scenarios; dashboard tests.
- `sql/`: analytical SQL; `contracts/`: JSON Schema; `examples/`: synthetic samples.
- `airflow/`: optional scheduled reconciliation.
- `.github/workflows/`: unit and container integration CI.
- `docs/`: architecture, setup, validation, limitations, and review.

## Technical references

Pinned versions are reproducibility choices, not assertions of newest releases.

- [Spark 3.5.6 Structured Streaming](https://spark.apache.org/docs/3.5.6/structured-streaming-programming-guide.html)
- [Spark Kafka integration](https://spark.apache.org/docs/3.5.6/structured-streaming-kafka-integration.html)
- [Iceberg Spark quickstart](https://iceberg.apache.org/spark-quickstart/)
- [Kafka Docker examples](https://github.com/apache/kafka/tree/trunk/docker/examples)

See [native results](docs/NATIVE_RESULTS.md) and validation notes for executed checks. No production use, throughput, fraud
accuracy, HIPAA compliance, or PCI compliance is claimed.
