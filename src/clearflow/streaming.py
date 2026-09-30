"""Bounded micro-batches: Kafka -> Iceberg raw -> relational domain processor.

The driver-side serving writer is deliberately single-threaded. This is a
portfolio workload, not a distributed high-throughput serving implementation.
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from clearflow.storage import get_engine
from clearflow.processor import ingest


def create_spark():
    builder = (SparkSession.builder.appName("clearflow-stream")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.shuffle.partitions", "2"))
    uri = os.getenv("ICEBERG_REST_URI")
    if uri:
        builder = (builder.config("spark.sql.catalog.lake.type", "rest")
            .config("spark.sql.catalog.lake.uri", uri)
            .config("spark.sql.catalog.lake.warehouse", os.environ["ICEBERG_WAREHOUSE"])
            .config("spark.sql.catalog.lake.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"))
    else:
        builder = (builder.config("spark.sql.catalog.lake.type", "hadoop")
            .config("spark.sql.catalog.lake.warehouse", os.getenv("ICEBERG_WAREHOUSE", "/app/data/warehouse")))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def main():
    spark = create_spark()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.bronze")
    spark.sql("""CREATE TABLE IF NOT EXISTS lake.bronze.raw_events
        (record_key STRING, topic STRING, partition INT, offset BIGINT, payload STRING,
         kafka_time TIMESTAMP) USING iceberg""")
    engine = get_engine()
    stream = (spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
        .option("subscribe", "banking.events.v1,healthcare.events.v1")
        .option("startingOffsets", "earliest").option("failOnDataLoss", "true")
        .option("maxOffsetsPerTrigger", 500).load()
        .select(F.concat_ws(":", "topic", F.col("partition").cast("string"),
                            F.col("offset").cast("string")).alias("record_key"),
                "topic", "partition", "offset", F.col("value").cast("string").alias("payload"),
                F.col("timestamp").alias("kafka_time")))

    def write_batch(batch, batch_id):
        batch.persist()
        try:
            batch.createOrReplaceTempView("incoming_raw")
            # Idempotent by Kafka position. Crash after the lake commit is safe to retry.
            # foreachBatch can receive a distinct SparkSession; use the view-owning session.
            batch.sparkSession.sql("""MERGE INTO lake.bronze.raw_events t USING incoming_raw s
                ON t.record_key = s.record_key WHEN NOT MATCHED THEN INSERT *""")
            for row in batch.orderBy("topic", "partition", "offset").toLocalIterator():
                ingest(engine, row.payload, source=row.topic, record_key="kafka:" + row.record_key)
            print(f"completed_batch={batch_id}", flush=True)
        finally:
            batch.unpersist()

    query = (stream.writeStream.foreachBatch(write_batch)
        .option("checkpointLocation", os.getenv("CHECKPOINT_PATH", "/app/data/checkpoints/main"))
        .trigger(processingTime="5 seconds").start())
    query.awaitTermination()

if __name__ == "__main__":
    main()
