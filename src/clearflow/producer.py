import csv
import json
import os
import time
from pathlib import Path
from kafka import KafkaProducer
from clearflow.generator import generate


def run(interval=0.5, count=0, output="data/live-reference.csv"):
    if interval < 0 or count < 0:
        raise ValueError("interval/count must be nonnegative")
    producer = KafkaProducer(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        acks="all", retries=10, max_in_flight_requests_per_connection=1,
        key_serializer=lambda x: x.encode(), value_serializer=lambda x: json.dumps(x).encode())
    path = Path(output)
    path.parent.mkdir(parents=True, exist_ok=True)
    produced = 0
    try:
        with path.open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["domain", "entity_id", "status", "amount", "currency"])
            writer.writeheader()
            while count == 0 or produced < count:
                size = min(10, count - produced) if count else 10
                records, reference = generate(size, seed=42 + produced)
                for record in records:
                    producer.send(f"{record['domain']}.events.v1", key=record["entity_id"], value=record).get(timeout=30)
                    time.sleep(interval)
                writer.writerows(reference)
                handle.flush()
                produced += size
                print(json.dumps({"entities_produced": produced}), flush=True)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()
