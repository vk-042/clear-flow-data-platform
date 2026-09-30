"""Full-snapshot reconciliation; reference must cover the same entity population."""
import csv
import json
from decimal import Decimal, InvalidOperation
from uuid import uuid4
from sqlalchemy import select
from clearflow.storage import entities, reconciliation
from clearflow.clock import now

def reconcile(engine, path):
    with open(path, newline="") as handle:
        rows = list(csv.DictReader(handle))
    expected = {}
    for row in rows:
        key = (row["domain"], row["entity_id"])
        if key in expected:
            raise ValueError(f"duplicate reference key: {key}")
        if row["domain"] not in ("banking", "healthcare") or row["currency"] != "USD":
            raise ValueError("unsupported reference domain/currency")
        try:
            amount = Decimal(row["amount"])
            if not amount.is_finite() or amount <= 0 or amount != amount.quantize(Decimal("0.01")):
                raise ValueError("invalid reference amount")
        except InvalidOperation as exc:
            raise ValueError("invalid reference amount") from exc
        expected[key] = (row["status"], int(amount * 100), row["currency"])
    with engine.begin() as conn:
        actual = {(r["domain"], r["entity_id"]): r for r in conn.execute(select(entities)).mappings()}
        issues = []
        for key in sorted(expected.keys() | actual.keys()):
            base = dict(domain=key[0], entity_id=key[1])
            if key not in actual:
                issues.append(dict(base, reason="missing_internal"))
            elif key not in expected:
                issues.append(dict(base, reason="missing_reference"))
            else:
                row = actual[key]
                if (row["status"], row["amount_cents"], row["currency"]) != expected[key]:
                    issues.append(dict(base, reason="state_or_amount_mismatch"))
                elif row["quality_issue"]:
                    issues.append(dict(base, reason="unresolved_lifecycle"))
        report = dict(run_id=uuid4().hex, created_at=now(), expected_count=len(expected),
                      internal_count=len(actual), matched=len(expected.keys() & actual.keys()) -
                      sum(x["reason"] in ("state_or_amount_mismatch", "unresolved_lifecycle") for x in issues),
                      issues=issues)
        conn.execute(reconciliation.insert().values(run_id=report["run_id"], created_at=report["created_at"],
                                                   report_json=json.dumps(report)))
    return report
