from copy import deepcopy
from decimal import Decimal
from sqlalchemy import select, func
from clearflow.storage import events, entities, raw
from clearflow.processor import ingest, rebuild_all


def state(engine):
    with engine.connect() as conn:
        return dict(conn.execute(select(entities)).mappings().one())

def count(engine, table):
    with engine.connect() as conn:
        return conn.scalar(select(func.count()).select_from(table))

def test_duplicate_does_not_inflate_amount(engine, journey):
    for row in journey:
        assert ingest(engine, row) == "accepted"
    assert ingest(engine, journey[-1], source="redelivery") == "duplicate"
    assert count(engine, events) == len(journey)
    assert count(engine, entities) == 1
    assert state(engine)["amount_cents"] == int(Decimal(journey[-1]["amount"]) * 100)

def test_reverse_arrival_recovers_state(engine, journey):
    assert ingest(engine, journey[-1]) == "accepted"
    assert state(engine)["status"] == "pending"
    assert state(engine)["quality_issue"] == "missing_sequence:1"
    for row in journey[:-1][::-1]:
        ingest(engine, row)
    assert state(engine)["status"] == journey[-1]["status"]
    assert state(engine)["quality_issue"] == ""

def test_conflicting_event_id_is_quarantined(engine, journey):
    ingest(engine, journey[0])
    conflict = dict(journey[0], amount="999.99")
    assert ingest(engine, conflict) == "quarantined"
    assert count(engine, events) == 1

def test_sequence_collision_is_quarantined(engine, journey):
    ingest(engine, journey[0])
    assert ingest(engine, dict(journey[0], event_id="another-id")) == "quarantined"

def test_invalid_money_and_unknown_fields(engine, journey):
    for patch in ({"amount": 1.25}, {"amount": "NaN"}, {"amount": "0.00"},
                  {"amount": "1.001"}, {"patient_name": "not allowed"}, {"schema_version": 2}):
        assert ingest(engine, dict(journey[0], **patch)) == "quarantined"
    assert count(engine, events) == 0

def test_poison_json_does_not_stop_next_event(engine, journey):
    assert ingest(engine, "not-json") == "quarantined"
    assert ingest(engine, journey[0]) == "accepted"

def test_record_replay_is_idempotent(engine, journey):
    assert ingest(engine, journey[0], record_key="topic:0:10") == "accepted"
    assert ingest(engine, journey[0], record_key="topic:0:10") == "replayed"
    assert count(engine, raw) == 1

def test_impossible_transition_is_visible(engine, journey):
    ingest(engine, journey[0])
    ingest(engine, dict(journey[1], status="settled"))
    assert state(engine)["status"] == "initiated"
    assert "invalid_transition" in state(engine)["quality_issue"]

def test_rebuild_reproduces_state(engine, journey):
    for row in journey:
        ingest(engine, row)
    before = state(engine)
    with engine.begin() as conn:
        conn.execute(entities.delete())
    assert rebuild_all(engine) == 1
    after = state(engine)
    for field in ("status", "sequence", "amount_cents", "quality_issue"):
        assert after[field] == before[field]

def test_cross_domain_source_rejected(engine, journey):
    assert ingest(engine, dict(journey[0], source_system="synthetic-claims")) == "quarantined"

def test_identity_change_rejected(engine, journey):
    ingest(engine, journey[0])
    assert ingest(engine, dict(journey[1], party_id="SYN-someone-else")) == "quarantined"

def test_transaction_rolls_back_on_failure(engine, journey, monkeypatch):
    import clearflow.processor as processor
    def broken(*args):
        raise RuntimeError("simulated failure before commit")
    monkeypatch.setattr(processor, "rebuild_entity", broken)
    import pytest
    with pytest.raises(RuntimeError):
        ingest(engine, journey[0])
    assert count(engine, events) == count(engine, raw) == 0

def test_timestamp_regression_is_flagged(engine, journey):
    ingest(engine, journey[0])
    row = deepcopy(journey[1])
    row["event_time"] = "2000-01-01T00:00:00+00:00"
    ingest(engine, row)
    assert "event_time_regression" in state(engine)["quality_issue"]
