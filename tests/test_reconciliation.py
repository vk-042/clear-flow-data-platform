import csv
from clearflow.generator import generate
from clearflow.processor import ingest
from clearflow.reconcile import reconcile


def write_reference(path, rows):
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

def test_both_domains_reconcile_despite_faults(engine, tmp_path):
    records, reference = generate(30, run_id="fixture")
    for row in records:
        ingest(engine, row)
    path = tmp_path / "reference.csv"
    write_reference(path, reference)
    report = reconcile(engine, path)
    assert report["matched"] == 30
    assert report["issues"] == []

def test_reconciliation_detects_missing_and_mismatch(engine, tmp_path):
    records, reference = generate(4, faults=False, run_id="fixture")
    for row in records:
        if row["entity_id"] != reference[0]["entity_id"]:
            ingest(engine, row)
    reference[1]["amount"] = "1.00"
    reference.pop()
    path = tmp_path / "reference.csv"
    write_reference(path, reference)
    reasons = {x["reason"] for x in reconcile(engine, path)["issues"]}
    assert reasons == {"missing_internal", "missing_reference", "state_or_amount_mismatch"}

def test_duplicate_reference_rejected(engine, tmp_path):
    import pytest
    _, rows = generate(1)
    path = tmp_path / "reference.csv"
    write_reference(path, rows + rows)
    with pytest.raises(ValueError, match="duplicate reference"):
        reconcile(engine, path)
