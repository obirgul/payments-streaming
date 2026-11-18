"""Shared helpers for Spark jobs: SparkSession, Kafka source, schema, progress listener."""
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.streaming import StreamingQueryListener

KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
DATA = "/data"  # output: silver parquet, delta, checkpoints (./data on the host)
REF = "/ref"    # static reference data (./spark/data on the host)

# event_time is deliberately a String: the producer writes ISO-8601, here we convert it to a timestamp
TXN_SCHEMA = T.StructType([
    T.StructField("txn_id", T.StringType()),
    T.StructField("wallet_id", T.StringType()),
    T.StructField("user_id", T.StringType()),
    T.StructField("amount", T.DoubleType()),
    T.StructField("currency", T.StringType()),
    T.StructField("merchant_id", T.StringType()),
    T.StructField("country", T.StringType()),
    T.StructField("channel", T.StringType()),
    T.StructField("event_time", T.StringType()),
])


def get_spark(app_name, delta=False):
    b = (SparkSession.builder.appName(app_name)
         .config("spark.sql.shuffle.partitions", "4")  # 200 partitions make no sense on a local machine
         .config("spark.sql.session.timeZone", "UTC"))
    if delta:
        b = (b.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def kafka_stream(spark, topic, starting="earliest"):
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA)
            .option("subscribe", topic)
            .option("startingOffsets", starting)
            .option("failOnDataLoss", "false")
            .load())


def parse_transactions(raw):
    """Frame with a Kafka-style `value` column -> typed transactions. Malformed (unparseable) records are dropped.
    In real life these would be written to a dead-letter topic."""
    return (raw.select(F.from_json(F.col("value").cast("string"), TXN_SCHEMA).alias("t"))
            .select("t.*")
            .where(F.col("txn_id").isNotNull() & F.col("wallet_id").isNotNull())
            .withColumn("event_time", F.to_timestamp("event_time")))


def read_transactions(spark):
    """Kafka `transactions` -> typed DataFrame (see parse_transactions)."""
    return parse_transactions(kafka_stream(spark, "transactions"))


class ProgressPrinter(StreamingQueryListener):
    """Prints a summary for each micro-batch. numRowsDroppedByWatermark = late events dropped because of the watermark."""

    def onQueryStarted(self, event):
        print("[query] basladi:", event.name or event.id)

    def onQueryProgress(self, event):
        p = event.progress
        dropped = 0
        for s in (p.stateOperators or []):
            dropped += getattr(s, "numRowsDroppedByWatermark", 0) or 0
        print("[batch %s] giris=%s satir (%.1f/s) | watermark=%s | watermark'la atilan=%s" % (
            p.batchId, p.numInputRows, p.inputRowsPerSecond or 0.0,
            (p.eventTime or {}).get("watermark"), dropped))

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        print("[query] bitti", "(hata: %s)" % event.exception if event.exception else "")


def finish(spark, listener):
    """Remove the listener cleanly after the query ends. The onQueryTerminated event is dispatched asynchronously in the JVM,
    so we wait briefly before the Python callback server shuts down; otherwise py4j prints a 'Received empty command' error."""
    time.sleep(3)
    spark.streams.removeListener(listener)
