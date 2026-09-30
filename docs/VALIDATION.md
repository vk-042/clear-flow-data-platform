# Validation report

Executed in the authoring workspace on 2026-09-25, using Python 3.12.14.

| Check | Result |
|---|---|
| Python test suite | 22 passed in 9.39 seconds (rerun after streaming fix) |
| Critical Ruff checks: E9, F63, F7, F82 | Passed |
| Python compilation: src, scripts, Airflow DAG | Passed |
| Compose YAML parse | Passed; not a Docker Compose runtime validation |
| Local 100-entity fixture | 312 records, 287 unique events, 15 duplicates, 10 quarantined |
| Reconciliation | 100 of 100 entities matched; zero findings |
| Cross-process JSONL replay | Zero newly accepted events; unique counts unchanged |
| SQLite database integrity after replay | ok |
| Dashboard test | Banking and healthcare rendered without exceptions |
| Native Kafka → Spark → Iceberg + SQLite | Passed, including recovery and lake counts |
| Docker Compose + PostgreSQL acceptance | Passed in GitHub Actions on 2026-09-30; 20 entities, then 40 after restart |
| Airflow DAG task execution | Passed via `airflow dags test`, 370 matched; SQLite |
| AWS S3/REST catalog | Not provisioned or tested |
| Native synthetic burst | 74.54 unique events/s; P95 10.71 s; 10-second target missed |

## Evidence

`verified-demo-results.json` captures local ingestion, counts, replay and reconciliation.
The replay report includes 287 duplicate unique events, 15 replayed duplicate records,
and 10 invalid records quarantined again under a new replay source. Accepted history
remains 287 events across 100 entities. Raw records increase from 312 to 609 because
new deliveries are audited. Local ingestion/reconciliation took 1.328 seconds in this
run; this is not a streaming benchmark.

The initial mounted-workspace run with SQLite WAL showed corruption on cross-process
reopen. Reproducing on /tmp isolated the storage-mode sensitivity. The implementation
now uses rollback journaling with FULL synchronization and explicitly disposes CLI
connections. Mounted-workspace replay and `PRAGMA integrity_check` then passed. A
cross-process CLI regression test is included.

## Covered failure cases

Duplicate and conflicting IDs, colliding entity sequences, invalid monetary inputs,
unknown schema fields/version, malformed JSON, missing predecessor recovery, invalid
transitions, timestamp regression, identity changes, transaction rollback, deterministic
rebuild, denied/adjusted healthcare claims, missing/mismatched references, duplicated
reference keys, anomaly score ordering, both dashboard views, and process restarts.

## Native integration evidence added

See `NATIVE_RESULTS.md` and `evidence/`. The native run used the actual Kafka broker,
Spark Structured Streaming connector, Iceberg runtime, and relational domain processor
with SQLite. It found and fixed a Spark-session mismatch: foreachBatch's temporary
view must be queried using `batch.sparkSession`, not the outer Spark session.
The final run reconciled all 370 entities after forced stop/backlog/restart and duplicate
redelivery. Iceberg bronze, silver and gold counts matched the serving store.
Airflow's actual DAG task subsequently reconciled all 370 entities successfully.

## Container acceptance completed

[GitHub Actions run 36792768299](https://github.com/vk-042/clear-flow-data-platform/actions/runs/36792768299)
passed on 2026-09-30 for commit `5641ca8cd4d2b46cdc47f803e85d7841ddc4d203`.
The Python job passed all 22 tests in 11.89 seconds, critical Ruff checks, the
100-entity demo, and Docker Compose configuration validation.

The integration job built the application image, started Kafka, PostgreSQL and
Spark, and reconciled all 20 entities from the first synthetic fixture. It then
restarted the processor using the persisted checkpoint and volumes, produced
another fixture, and reconciled all 40 entities with zero findings. The job logs
contain `PASS: 20 entities match Kafka reference snapshot` and
`PASS: 40 entities match Kafka reference snapshot`.

This verifies container startup, connector resolution, mounted-volume access,
SQL ingestion, reconciliation and processor restart for a small fixture. It does
not measure PostgreSQL throughput, verify every Iceberg table in the container,
exercise an in-flight commit failure, or test the Airflow container, dashboard
browser, cloud deployment or production security. Native lakehouse counts and
latency measurements remain the separate experiment documented in `NATIVE_RESULTS.md`.

## Publication review

The Python suite passed again: 22 tests in 9.82 seconds; critical Ruff checks passed.
A credential-pattern scan found no private keys or recognized token patterns. Compose
now runs a single database-initialization service before processor/dashboard startup,
removing their concurrent schema-creation race. The integration wait now allows five
minutes for first-start connector downloads. These container startup changes passed the GitHub Actions acceptance run above.
