"""Seeded synthetic fixtures plus continuously generated event lifecycles."""
import random
from datetime import datetime, timezone, timedelta
from uuid import uuid4

PATHS = {"banking": [("initiated", "authorized", "settled"), ("initiated", "declined"),
                     ("initiated", "authorized", "settled", "reversed")],
         "healthcare": [("submitted", "accepted", "paid"), ("submitted", "denied"),
                        ("submitted", "accepted", "paid", "adjusted")]}

def generate(count=100, seed=42, faults=True, run_id=None):
    rng = random.Random(seed)
    run_id = run_id or uuid4().hex[:10]
    start = datetime.now(timezone.utc) - timedelta(minutes=10)
    records, reference = [], []
    for i in range(count):
        domain = "banking" if i % 2 == 0 else "healthcare"
        path = PATHS[domain][rng.choices([0, 1, 2], [0.7, 0.2, 0.1])[0]]
        amount = rng.randint(1000, 35000) if domain == "banking" else rng.randint(15000, 450000)
        if i % 23 == 0:
            amount *= 40
        entity = f"{domain[:3]}-{run_id}-{i:05d}"
        journey = []
        for sequence, status in enumerate(path, 1):
            # A claim adjustment changes the outstanding business amount; bank reversals retain face value.
            current_amount = amount - 1000 if status == "adjusted" else amount
            journey.append(dict(schema_version=1, event_id=f"{entity}-{sequence}", domain=domain,
                entity_id=entity, party_id=f"SYN-{domain}-{i % 20:03d}",
                source_system="synthetic-bank" if domain == "banking" else "synthetic-claims",
                sequence=sequence, status=status, amount=f"{current_amount // 100}.{current_amount % 100:02d}",
                currency="USD", event_time=(start + timedelta(seconds=i + sequence)).isoformat()))
        reference.append({k: journey[-1][k] for k in ("domain", "entity_id", "status", "amount", "currency")})
        if faults and i % 5 == 0:
            journey = journey[::-1]
        records.extend(journey)
        if faults and i % 7 == 0:
            records.append(dict(journey[0]))
        if faults and i % 11 == 0:
            bad = dict(journey[0], event_id=f"bad-{entity}", amount="-1.00")
            records.append(bad)
    return records, reference
