import argparse
import csv
import json
import time
from pathlib import Path
from sqlalchemy import select, func
from clearflow.generator import generate
from clearflow.storage import get_engine, events, entities, raw
from clearflow.processor import ingest, rebuild_all
from clearflow.reconcile import reconcile


def main():
    parser = argparse.ArgumentParser(description="ClearFlow synthetic streaming platform")
    parser.add_argument("--database-url", default=None)
    commands = parser.add_subparsers(dest="command", required=True)
    demo = commands.add_parser("demo")
    demo.add_argument("--count", type=int, default=100)
    demo.add_argument("--seed", type=int, default=42)
    demo.add_argument("--output", default="data")
    replay = commands.add_parser("replay")
    replay.add_argument("path")
    rec = commands.add_parser("reconcile")
    rec.add_argument("path")
    commands.add_parser("rebuild")
    commands.add_parser("stats")
    produce = commands.add_parser("produce")
    produce.add_argument("--interval", type=float, default=0.5)
    produce.add_argument("--count", type=int, default=0, help="0 means continuous; otherwise number of entities")
    produce.add_argument("--output", default="data/live-reference.csv")
    args = parser.parse_args()
    if args.command == "produce":
        from clearflow.producer import run
        run(args.interval, args.count, args.output)
        return
    engine = get_engine(args.database_url)
    if args.command == "demo":
        if args.count < 1 or args.count > 400:
            parser.error("demo count must be between 1 and 400")
        output = Path(args.output)
        output.mkdir(parents=True, exist_ok=True)
        records, reference = generate(args.count, args.seed)
        path = output / "events.jsonl"
        path.write_text("".join(json.dumps(row) + "\n" for row in records))
        with (output / "reference.csv").open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(reference[0]))
            writer.writeheader()
            writer.writerows(reference)
        started = time.perf_counter()
        results = {}
        for i, record in enumerate(records):
            result = ingest(engine, record, source=str(path), record_key=f"{path}:{records[0]['event_id']}:{i}")
            results[result] = results.get(result, 0) + 1
        report = reconcile(engine, output / "reference.csv")
        elapsed = time.perf_counter() - started
        print(json.dumps(dict(records=len(records), outcomes=results, elapsed_seconds=round(elapsed, 3),
                              reconciliation=report), indent=2))
    elif args.command == "replay":
        results = {}
        with open(args.path) as handle:
            for line in handle:
                result = ingest(engine, line.strip(), source="replay")
                results[result] = results.get(result, 0) + 1
        print(json.dumps(results, indent=2))
    elif args.command == "reconcile":
        print(json.dumps(reconcile(engine, args.path), indent=2))
    elif args.command == "rebuild":
        print(json.dumps({"rebuilt_entities": rebuild_all(engine)}))
    elif args.command == "stats":
        with engine.connect() as conn:
            print(json.dumps({table.name: conn.scalar(select(func.count()).select_from(table))
                              for table in (raw, events, entities)}, indent=2))

    engine.dispose()

if __name__ == "__main__":
    main()
