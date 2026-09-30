"""Reproducible native Kafka/Spark/Iceberg + SQLite integration experiment.

Requires Java 17, a downloaded Kafka 3.9.1 distribution and .[spark,dev].
Creates only synthetic data. Does not validate Docker, PostgreSQL, Airflow or AWS.
"""
import argparse
import csv
import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import numpy as np
from kafka import KafkaProducer
from sqlalchemy import select, func
from clearflow.generator import generate
from clearflow.processor import now
from clearflow.reconcile import reconcile
from clearflow.storage import get_engine, events, entities, raw

REPO = Path(__file__).resolve().parents[1]
PACKAGES = ("org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,"
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--kafka-home", required=True, type=Path)
    parser.add_argument("--work-dir", required=True, type=Path)
    parser.add_argument("--count", type=int, default=300)
    parser.add_argument("--port", type=int, default=19094)
    parser.add_argument("--jars-dir", type=Path, help="Optional predownloaded Spark connector jars")
    args = parser.parse_args()
    work = args.work_dir.resolve()
    work.mkdir(parents=True, exist_ok=False)
    if not 10 <= args.count <= 1000:
        raise ValueError("Use 10–1000 entities for this bounded demo")
    import pyspark
    spark_submit = str(Path(pyspark.__file__).parent / "bin/spark-submit")
    bootstrap = f"127.0.0.1:{args.port}"
    config = work / "server.properties"
    config.write_text(f"""process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:{args.port+1}
listeners=PLAINTEXT://127.0.0.1:{args.port},CONTROLLER://127.0.0.1:{args.port+1}
advertised.listeners=PLAINTEXT://{bootstrap}
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
log.dirs={work / 'kafka-data'}
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
auto.create.topics.enable=false
group.initial.rebalance.delay.ms=0
""")
    ivy = work / "ivysettings.xml"
    ivy.write_text('<ivysettings><settings defaultResolver="central"/><resolvers>'
                  '<ibiblio name="central" m2compatible="true" '
                  'root="https://repo.maven.apache.org/maven2/"/></resolvers></ivysettings>')
    env = dict(os.environ, DATABASE_URL=f"sqlite:///{work / 'serving.db'}",
               KAFKA_BOOTSTRAP_SERVERS=bootstrap, ICEBERG_WAREHOUSE=str(work / "warehouse"),
               CHECKPOINT_PATH=str(work / "checkpoints"), SPARK_LOCAL_IP="127.0.0.1",
               PYTHONPATH=str(REPO / "src"), PYSPARK_PYTHON=sys.executable,
               KAFKA_HEAP_OPTS="-Xmx512m -Xms256m", PYTHONUNBUFFERED="1")
    engine = get_engine(env["DATABASE_URL"])
    children, handles = [], []
    def launch(command, name):
        handle = (work / f"{name}.log").open("w")
        handles.append(handle)
        child = subprocess.Popen(command, env=env, stdout=handle, stderr=subprocess.STDOUT,
                                 start_new_session=True, cwd=REPO)
        children.append(child)
        return child
    def stop(child, hard=False):
        if child.poll() is None:
            os.killpg(child.pid, signal.SIGKILL if hard else signal.SIGTERM)
            try:
                child.wait(timeout=15)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGKILL)
                child.wait(timeout=10)
    def command(script, *args):
        return [str(args_kafka / "bin" / script), *map(str, args)]
    args_kafka = args.kafka_home.resolve()
    subprocess.run(command("kafka-storage.sh", "format", "-t", "MkU3OEVBNTcwNTJENDM2Qk", "-c", config),
                   env=env, check=True, capture_output=True)
    spark_command = [spark_submit, "--master", "local[2]", "--driver-memory", "1g",
        "--conf", f"spark.jars.ivySettings={ivy}", "--packages", PACKAGES,
        str(REPO / "src/clearflow/streaming.py")]
    if args.jars_dir:
        jars = sorted(args.jars_dir.resolve().glob("*.jar"))
        if not jars:
            raise ValueError("No jars found")
        index = spark_command.index("--packages")
        spark_command[index:index+2] = ["--jars", ",".join(map(str, jars))]
    producer = None
    monitor_stop = threading.Event()
    starts, observed = {}, {}
    monitor_errors = []
    def monitor():
        try:
            while not monitor_stop.wait(0.1):
                with engine.connect() as conn:
                    ids = conn.execute(select(events.c.event_id)).scalars().all()
                instant = time.perf_counter()
                for identity in ids:
                    if identity in starts and identity not in observed:
                        observed[identity] = instant - starts[identity]
        except Exception as exc:
            monitor_errors.append(repr(exc))
    thread = threading.Thread(target=monitor, daemon=True)
    reports = {"created_at": now(), "mode": "native Kafka/Spark/Iceberg/SQLite",
               "excluded": ["Docker", "PostgreSQL", "Airflow", "AWS"], "phases": {}}
    references, all_valid = [], set()
    sent_records = 0
    def fixture(count, label):
        rows, reference = generate(count, seed=42, run_id=label)
        # Source timestamps deliberately close to publication, monotonically increasing within each entity.
        base = datetime.now(timezone.utc) - timedelta(seconds=1)
        for row in rows:
            row["event_time"] = (base + timedelta(milliseconds=row["sequence"])).isoformat()
        return rows, reference
    def send(rows):
        nonlocal sent_records
        for row in rows:
            if row["amount"] != "-1.00":
                starts.setdefault(row["event_id"], time.perf_counter())
                all_valid.add(row["event_id"])
            producer.send(f"{row['domain']}.events.v1", key=row["entity_id"], value=row).get(timeout=30)
            sent_records += 1
        producer.flush()
    def wait_for_output(process, timeout=240):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError("Spark exited; inspect processor log")
            if monitor_errors:
                raise RuntimeError(monitor_errors)
            with engine.connect() as conn:
                accepted = set(conn.execute(select(events.c.event_id)).scalars())
                deliveries = conn.scalar(select(func.count()).select_from(raw))
            if accepted == all_valid and deliveries >= sent_records:
                return
            time.sleep(0.2)
        raise TimeoutError(f"Stream did not drain; inspect {work}")
    def verify_reference(label):
        path = work / "reference.csv"
        with path.open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(references[0]))
            writer.writeheader()
            writer.writerows(references)
        report = reconcile(engine, path)
        if report["issues"]:
            raise AssertionError(report)
        print(f"{label}: matched {report['matched']} entities", flush=True)
        return report
    try:
        broker = launch(command("kafka-server-start.sh", config), "kafka")
        deadline = time.monotonic() + 60
        while True:
            if broker.poll() is not None:
                raise RuntimeError("Kafka exited; inspect kafka.log")
            try:
                with socket.create_connection(("127.0.0.1", args.port), timeout=1):
                    break
            except OSError:
                if time.monotonic() > deadline:
                    raise TimeoutError("Kafka startup")
                time.sleep(0.5)
        for domain in ("banking", "healthcare"):
            subprocess.run(command("kafka-topics.sh", "--bootstrap-server", bootstrap, "--create",
                           "--topic", f"{domain}.events.v1", "--partitions", 3, "--replication-factor", 1),
                           env=env, check=True, capture_output=True)
        producer = KafkaProducer(bootstrap_servers=bootstrap, acks="all", retries=5,
            key_serializer=lambda x: x.encode(), value_serializer=lambda x: json.dumps(x).encode())
        processor = launch(spark_command, "processor-first")
        thread.start()
        rows, reference = fixture(20, "warmup")
        references.extend(reference)
        started = time.perf_counter()
        send(rows)
        wait_for_output(processor, timeout=360)
        reports["phases"]["warmup"] = {"seconds": time.perf_counter()-started,
                                          "reconciliation": verify_reference("warmup")}
        # Wait for Spark to write its batch checkpoint before the measured burst.
        time.sleep(2)
        rows, reference = fixture(args.count, "measured")
        references.extend(reference)
        ids = {r["event_id"] for r in rows if r["amount"] != "-1.00"}
        started = time.perf_counter()
        send(rows)
        wait_for_output(processor)
        elapsed = time.perf_counter() - started
        deadline = time.monotonic()+5
        while not ids.issubset(observed) and time.monotonic()<deadline:
            time.sleep(0.1)
        latencies = [observed[key] for key in ids]
        reports["phases"]["measured_burst"] = {
            "input_records": len(rows), "unique_valid_events": len(ids), "entities": args.count,
            "seconds_to_drain": elapsed, "unique_events_per_second": len(ids)/elapsed,
            "latency_seconds": {f"p{p}": float(np.percentile(latencies, p)) for p in (50,95,99)},
            "latency_definition": "before Kafka send to first observed committed SQL event; 100ms polling",
            "reconciliation": verify_reference("measured")}
        print(json.dumps(reports["phases"]["measured_burst"], indent=2), flush=True)
        # Hard crash, queue a backlog, restart with the same checkpoint and storage.
        stop(processor, hard=True)
        rows, reference = fixture(50, "recovery")
        references.extend(reference)
        send(rows)
        with engine.connect() as conn:
            before = conn.scalar(select(func.count()).select_from(events))
        time.sleep(2)
        with engine.connect() as conn:
            assert before == conn.scalar(select(func.count()).select_from(events))
        restarted = time.perf_counter()
        processor = launch(spark_command, "processor-restart")
        wait_for_output(processor)
        reports["phases"]["crash_recovery"] = {
            "backlog_records": len(rows), "seconds_from_restart_to_drain": time.perf_counter()-restarted,
            "reconciliation": verify_reference("crash recovery")}
        # Replay already accepted measured IDs at NEW Kafka offsets.
        # Preserve original timestamps/content so these are true duplicate IDs, not conflicts.
        with engine.connect() as conn:
            payloads = [json.loads(x) for x in conn.execute(select(events.c.payload)
                        .where(events.c.entity_id.like('%-measured-%')).limit(20)).scalars()]
        send(payloads)
        wait_for_output(processor)
        reports["phases"]["duplicate_redelivery"] = {"resent":len(payloads),
            "reconciliation":verify_reference("redelivery")}
        time.sleep(2)
        stop(processor)
        monitor_stop.set()
        thread.join(timeout=5)
        with engine.connect() as conn:
            reports["final_sql_counts"] = {t.name:conn.scalar(select(func.count()).select_from(t))
                                           for t in (raw,events,entities)}
            reports["raw_outcomes"] = dict(conn.execute(select(raw.c.outcome,func.count())
                                                       .group_by(raw.c.outcome)).all())
        snapshot_command = spark_command[:-1]+[str(REPO/'src/clearflow/lakehouse_snapshot.py')]
        snapshot = launch(snapshot_command,"snapshot")
        if snapshot.wait(timeout=180) != 0:
            raise RuntimeError("Snapshot export failed")
        check_script = work/'inspect_lake.py'
        check_script.write_text('''import json
from clearflow.streaming import create_spark
spark=create_spark()
result={name:spark.table("lake."+name).count() for name in ["bronze.raw_events","silver.entities","gold.current_totals"]}
assert spark.sql("SELECT count(*) FROM lake.bronze.raw_events").first()[0] == spark.sql("SELECT count(DISTINCT record_key) FROM lake.bronze.raw_events").first()[0]
result["gold_entity_count"]=spark.sql("SELECT sum(entity_count) FROM lake.gold.current_totals").first()[0]
print("LAKE_RESULT="+json.dumps(result),flush=True)
spark.stop()
''')
        inspector=launch(spark_command[:-1]+[str(check_script)],'lake-inspection')
        if inspector.wait(timeout=180)!=0:
            raise RuntimeError("Lake inspection failed")
        line=next(x for x in (work/'lake-inspection.log').read_text().splitlines() if x.startswith('LAKE_RESULT='))
        reports['iceberg']=json.loads(line.split('=',1)[1])
        assert reports['iceberg']['bronze.raw_events']==reports['final_sql_counts']['raw_records']
        assert reports['iceberg']['silver.entities']==len(references)
        assert reports['iceberg']['gold_entity_count']==len(references)
        reports['status']='passed'
        (work/'results.json').write_text(json.dumps(reports,indent=2))
        print(json.dumps(reports,indent=2),flush=True)
    finally:
        monitor_stop.set()
        if producer:
            producer.close(timeout=5)
        for child in reversed(children):
            stop(child)
        for handle in handles:
            handle.close()
        engine.dispose()

if __name__ == '__main__':
    main()
