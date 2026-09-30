# Data dictionary

| Event field | Rule |
|---|---|
| schema_version | Exactly 1 |
| event_id | Immutable global event ID, up to 100 safe characters |
| domain | banking or healthcare |
| entity_id | Payment/claim ID within domain |
| party_id | Synthetic account/provider ID starting SYN- |
| source_system | synthetic-bank or synthetic-claims, matching domain |
| sequence | Strict integer 1–10000, contiguous within entity |
| status | Valid domain lifecycle status |
| amount | Positive decimal string, at most two fractional digits |
| currency | USD |
| event_time | Timezone-aware business timestamp, normalized to UTC |

JSON Schema is in `contracts/event-v1.schema.json`. Cross-field constraints also
run in Python. Timestamps more than five minutes into the future are rejected.

## Tables

- `raw_records`: source, raw text, record key, received time, outcome, rejection reason.
- `events`: unique accepted event, unique entity sequence, exact cents, canonical
  fingerprint, event/ingestion time, score/flag/model version, normalized payload.
- `entities`: domain/entity key, last contiguous valid state, cents, party, update time,
  unresolved quality issue. Pending entities are excluded from settled/paid totals.
- `reconciliation_runs`: run ID, time, and full report JSON.

## Metrics

- Distinct payments/claims: entity count including pending.
- Currently settled/paid: issue-free entities in exactly that status; sum cents / 100.
  Reversals and adjustments appear in their own status, excluded from these totals.
- Flagged entities: distinct IDs with at least one anomalous event.
- Lifecycle issues: current entity rows with nonempty quality issue.
- P95 event-to-ingestion delay: includes source delay; not Kafka lag or a streaming benchmark.
- Matched references: identical status/amount/currency and no unresolved lifecycle issue.

Reference CSV is a complete snapshot of the same entity population as the database.
Duplicate reference keys fail validation. Missing internal, missing reference, monetary/
status mismatch, and unresolved lifecycle issues are separate findings.
