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
| Docker Compose + PostgreSQL acceptance | Not executed: container runtime unavailable |
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

## Remaining release gate

Run the full container acceptance workflow in GitHub Actions or on a Docker host.
It produces a finite fixture, verifies both domains, restarts the processor, produces
another fixture, and verifies all 40 entities. Dependency installation, JVM connector
resolution, image startup, mounted-volume permissions, and checkpoint recovery in the Docker/PostgreSQL configuration
remain unverified until that workflow passes. Do not describe the stack as production
ready or full-stack tested before then.

## Publication review

The Python suite passed again: 22 tests in 9.82 seconds; critical Ruff checks passed.
A credential-pattern scan found no private keys or recognized token patterns. Compose
now runs a single database-initialization service before processor/dashboard startup,
removing their concurrent schema-creation race. The integration wait now allows five
minutes for first-start connector downloads. These container startup changes still
need GitHub Actions verification; they are not described as already runtime tested.
