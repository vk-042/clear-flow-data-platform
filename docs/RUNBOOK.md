# Operations runbook

## Completion criteria

Both domain dashboards populate. Reference reconciliation matches. Duplicates do not
inflate history/totals. Reverse arrival converges. Bad amounts are quarantined.
Restart preserves state and checkpoints.

```bash
docker compose ps
docker compose logs --tail=100 processor
docker compose run --rm dashboard python -m clearflow.cli stats
docker compose exec postgres psql -U clearflow -d clearflow -c "SELECT outcome, count(*) FROM raw_records GROUP BY outcome;"
```

Connector downloads need Maven access. A created container is not proof of successful
streaming: inspect `completed_batch=` logs and reconciliation. Offset loss fails the
stream (`failOnDataLoss=true`); investigate before deleting a checkpoint.

## Failure drills

1. Replay local JSONL; accepted events/entities must stay constant.
2. Stop PostgreSQL, observe processing fail, then restore PostgreSQL/processor.
3. Stop processor while producer runs; restart and inspect backlog catch-up.
4. Send final sequence first, then predecessors; pending state must resolve.
5. Alter a reference amount; report must identify `state_or_amount_mismatch`.

CI tests container startup, reconciliation and restart. Outage/backlog throughput
remains an operator drill, not a claim established by Python tests.

## Rebuild and replay

Stop the writer before `python -m clearflow.cli rebuild`. It recomputes state from
accepted history in one transaction. Larger stores should use versioned shadow tables.
`replay` applies normal validation/deduplication to JSONL. Use a separate database for
experiments. Export raw payloads from Iceberg for source-level backfill when required.

## Curated Iceberg snapshot

```bash
docker compose run --rm processor spark-submit --master 'local[2]' --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.iceberg:iceberg-aws-bundle:1.8.1 /app/src/clearflow/lakehouse_snapshot.py
```

The bounded export reads relational state consistently and overwrites silver entities
and gold totals as new Iceberg snapshots. Cross-table commits are separate. Pause the
writer for a stable demo; production needs cross-table version coordination.

## Reference scope

Local fixture CSV covers only its own run. Continuous producer reference starts again
on process restart, so do not treat it as complete history after restart. Stop the
producer and retain a full snapshot before reconciliation. Real source adapters should
supply business-date partitioned settlement/remittance extracts, as-of time, control
totals, and incomplete-period semantics.
