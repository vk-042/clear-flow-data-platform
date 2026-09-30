"""Shared relational schema; money is stored as integer cents."""
import os
from pathlib import Path
from sqlalchemy import (MetaData, Table, Column, String, Integer, BigInteger, Float, Text,
                        UniqueConstraint, create_engine, event)

metadata = MetaData()
raw = Table("raw_records", metadata,
    Column("record_id", String(64), primary_key=True),
    Column("source", String(240), nullable=False), Column("payload", Text, nullable=False),
    Column("received_at", String(40), nullable=False),
    Column("outcome", String(40), nullable=False), Column("reason", Text))
events = Table("events", metadata,
    Column("event_id", String(100), primary_key=True),
    Column("fingerprint", String(64), nullable=False),
    Column("domain", String(20), nullable=False), Column("entity_id", String(100), nullable=False),
    Column("sequence", Integer, nullable=False), Column("status", String(30), nullable=False),
    Column("amount_cents", BigInteger, nullable=False), Column("currency", String(3), nullable=False),
    Column("party_id", String(100), nullable=False), Column("event_time", String(40), nullable=False),
    Column("received_at", String(40), nullable=False), Column("payload", Text, nullable=False),
    Column("anomaly_score", Float, nullable=False), Column("is_anomaly", Integer, nullable=False),
    Column("model_version", String(60), nullable=False),
    UniqueConstraint("domain", "entity_id", "sequence", name="uq_entity_sequence"))
entities = Table("entities", metadata,
    Column("domain", String(20), primary_key=True), Column("entity_id", String(100), primary_key=True),
    Column("status", String(30), nullable=False), Column("sequence", Integer, nullable=False),
    Column("amount_cents", BigInteger, nullable=False), Column("currency", String(3), nullable=False),
    Column("party_id", String(100), nullable=False), Column("event_time", String(40), nullable=False),
    Column("updated_at", String(40), nullable=False), Column("quality_issue", Text, nullable=False))
reconciliation = Table("reconciliation_runs", metadata,
    Column("run_id", String(40), primary_key=True), Column("created_at", String(40), nullable=False),
    Column("report_json", Text, nullable=False))

def get_engine(url=None):
    url = url or os.getenv("DATABASE_URL", "sqlite:///data/clearflow.db")
    if url.startswith("sqlite:///") and not url.endswith(":memory:"):
        Path(url.removeprefix("sqlite:///")).parent.mkdir(parents=True, exist_ok=True)
    engine = create_engine(url, pool_pre_ping=True)
    if engine.dialect.name == "sqlite":
        @event.listens_for(engine, "connect")
        def configure(dbapi, _):
            # Rollback journal also works on mounted workspaces without WAL shared-memory support.
            dbapi.execute("PRAGMA journal_mode=DELETE")
            dbapi.execute("PRAGMA synchronous=FULL")
            dbapi.execute("PRAGMA busy_timeout=30000")
    metadata.create_all(engine)
    return engine
