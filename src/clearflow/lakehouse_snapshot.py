"""Bounded full export for the demo, not an incremental production CDC job."""
from sqlalchemy import select
from pyspark.sql import functions as F
from clearflow.storage import get_engine, entities
from clearflow.streaming import create_spark


def main():
    engine = get_engine()
    options = {"isolation_level": "REPEATABLE READ"} if engine.dialect.name == "postgresql" else {}
    with engine.connect().execution_options(**options) as conn, conn.begin():
        rows = [tuple(row) for row in conn.execute(select(entities))]
    spark = create_spark()
    schema = ("domain string, entity_id string, status string, sequence int, amount_cents long, "
              "currency string, party_id string, event_time string, updated_at string, quality_issue string")
    frame = spark.createDataFrame(rows, schema)
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.silver")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.gold")
    frame.writeTo("lake.silver.entities").using("iceberg").createOrReplace()
    totals = (frame.where(F.col("quality_issue") == "")
        .groupBy("domain", "status", "currency")
        .agg(F.count("*").alias("entity_count"), F.sum("amount_cents").alias("total_cents")))
    totals.writeTo("lake.gold.current_totals").using("iceberg").createOrReplace()
    print(f"Exported {len(rows)} entities to silver and refreshed gold totals")
    spark.stop()

if __name__ == "__main__":
    main()
