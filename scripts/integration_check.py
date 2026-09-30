"""Poll until complete reference snapshots reconcile; fail on timeout."""
import argparse
import csv
import tempfile
import time
from pathlib import Path
from clearflow.storage import get_engine
from clearflow.reconcile import reconcile


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--reference", action="append")
    parser.add_argument("--expected", type=int, default=20)
    parser.add_argument("--timeout", type=float, default=300, help="Seconds to allow first-start connector downloads")
    args = parser.parse_args()
    paths = args.reference or ["/app/data/reference.csv"]
    rows = []
    for path in paths:
        with open(path, newline="") as handle:
            rows.extend(csv.DictReader(handle))
    if not rows:
        raise SystemExit("FAIL: empty reference")
    engine = get_engine()
    with tempfile.TemporaryDirectory() as folder:
        reference = Path(folder) / "combined.csv"
        with reference.open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
        deadline = time.monotonic() + args.timeout
        while time.monotonic() < deadline:
            report = reconcile(engine, reference)
            if not report["issues"] and report["expected_count"] == args.expected:
                print(f"PASS: {report['matched']} entities match Kafka reference snapshot")
                break
            time.sleep(2)
        else:
            raise SystemExit(f"FAIL: {report}")
    engine.dispose()

if __name__ == "__main__":
    main()
