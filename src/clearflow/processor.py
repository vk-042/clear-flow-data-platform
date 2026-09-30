"""Atomic relational ingestion. Single writer per deployment is required."""
import hashlib
import json
from datetime import datetime, timezone, timedelta
from sqlalchemy import select, and_, delete
from pydantic import ValidationError
from clearflow.contracts import Event, LIFECYCLES
from clearflow.storage import raw, events, entities
from clearflow.anomaly import score

def now():
    return datetime.now(timezone.utc).isoformat()

def entity_filter(domain, entity_id):
    return and_(entities.c.domain == domain, entities.c.entity_id == entity_id)

def rebuild_entity(conn, domain, entity_id):
    history = conn.execute(select(events).where(and_(events.c.domain == domain,
        events.c.entity_id == entity_id)).order_by(events.c.sequence)).mappings().all()
    status, sequence, chosen, issue = None, 0, None, ""
    previous_time = None
    for row in history:
        if row["sequence"] != sequence + 1:
            issue = f"missing_sequence:{sequence + 1}"
            break
        if row["status"] not in LIFECYCLES[domain][status]:
            issue = f"invalid_transition:{status}->{row['status']}"
            break
        if previous_time and row["event_time"] < previous_time:
            issue = f"event_time_regression:{row['sequence']}"
            break
        status, sequence, chosen = row["status"], row["sequence"], row
        previous_time = row["event_time"]
    # A future event remains in history until its missing predecessors arrive.
    display = chosen or history[0]
    values = dict(domain=domain, entity_id=entity_id, status=status or "pending",
        sequence=sequence, amount_cents=display["amount_cents"], currency=display["currency"],
        party_id=display["party_id"], event_time=display["event_time"], updated_at=now(), quality_issue=issue)
    conn.execute(delete(entities).where(entity_filter(domain, entity_id)))
    conn.execute(entities.insert().values(**values))

def ingest(engine, payload, source="local", record_key=None, received_at=None):
    text = payload if isinstance(payload, str) else json.dumps(payload, sort_keys=True)
    record_id = hashlib.sha256((record_key or (source + ":" + text)).encode()).hexdigest()
    received_at = received_at or now()
    with engine.begin() as conn:
        prior = conn.execute(select(raw).where(raw.c.record_id == record_id)).mappings().first()
        if prior:
            if prior["payload"] != text:
                raise ValueError("record key reused with different payload")
            return "replayed"
        conn.execute(raw.insert().values(record_id=record_id, source=source, payload=text,
                                        received_at=received_at, outcome="received"))
        def finish(outcome, reason=None):
            conn.execute(raw.update().where(raw.c.record_id == record_id).values(outcome=outcome, reason=reason))
            return outcome
        try:
            ev = Event.model_validate_json(text)
        except (ValidationError, ValueError):
            # Do not copy arbitrary payload values into error logs.
            return finish("quarantined", "schema_validation_failed")
        if ev.event_time > datetime.fromisoformat(received_at) + timedelta(minutes=5):
            return finish("quarantined", "event_time_too_far_in_future")
        existing = conn.execute(select(events).where(events.c.event_id == ev.event_id)).mappings().first()
        if existing:
            return finish("duplicate" if existing["fingerprint"] == ev.fingerprint() else "quarantined",
                          None if existing["fingerprint"] == ev.fingerprint() else "event_id_conflict")
        history = conn.execute(select(events).where(and_(events.c.domain == ev.domain,
            events.c.entity_id == ev.entity_id))).mappings().all()
        if any(row["sequence"] == ev.sequence for row in history):
            return finish("quarantined", "sequence_conflict")
        if any(row["party_id"] != ev.party_id or row["currency"] != ev.currency for row in history):
            return finish("quarantined", "entity_identity_conflict")
        if ev.domain == "banking" and any(row["amount_cents"] != int(ev.amount * 100) for row in history):
            return finish("quarantined", "banking_amount_conflict")
        anomaly, flag, version = score(ev.domain, int(ev.amount * 100))
        conn.execute(events.insert().values(event_id=ev.event_id, fingerprint=ev.fingerprint(),
            domain=ev.domain, entity_id=ev.entity_id, sequence=ev.sequence, status=ev.status,
            amount_cents=int(ev.amount * 100), currency=ev.currency, party_id=ev.party_id,
            event_time=ev.event_time.isoformat(), received_at=received_at,
            payload=json.dumps(ev.wire(), sort_keys=True), anomaly_score=anomaly,
            is_anomaly=flag, model_version=version))
        rebuild_entity(conn, ev.domain, ev.entity_id)
        return finish("accepted")

def rebuild_all(engine):
    with engine.begin() as conn:
        pairs = conn.execute(select(events.c.domain, events.c.entity_id).distinct()).all()
        for domain, entity_id in pairs:
            rebuild_entity(conn, domain, entity_id)
    return len(pairs)
