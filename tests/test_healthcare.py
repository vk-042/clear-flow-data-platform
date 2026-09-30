from decimal import Decimal
from sqlalchemy import select
from clearflow.generator import generate
from clearflow.processor import ingest
from clearflow.storage import entities, events


def test_adjusted_claim_uses_latest_amount(engine):
    records, _ = generate(2, faults=False, run_id="claims")
    base = next(row for row in records if row["domain"] == "healthcare")
    journey = [dict(base, event_id=f"claim-{i}", sequence=i, status=status,
                    amount="900.00" if status == "adjusted" else "1000.00")
               for i, status in enumerate(["submitted", "accepted", "paid", "adjusted"], 1)]
    adjusted = journey[-1]
    for row in reversed(journey):
        ingest(engine, row)
    with engine.connect() as conn:
        entity = conn.execute(select(entities)).mappings().one()
        assert entity["status"] == "adjusted"
        assert entity["amount_cents"] == int(Decimal(adjusted["amount"]) * 100)
        assert entity["quality_issue"] == ""
        assert len(conn.execute(select(events)).all()) == len(journey)

def test_denied_claim_remains_denied(engine):
    records, _ = generate(30, faults=False, run_id="claims")
    denied = next(row for row in records if row["status"] == "denied")
    for row in records:
        if row["entity_id"] == denied["entity_id"]:
            ingest(engine, row)
    with engine.connect() as conn:
        assert conn.execute(select(entities.c.status)).scalar_one() == "denied"
