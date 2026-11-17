"""Spark isleri icin ortak yardimcilar: SparkSession, Kafka kaynagi, sema, ilerleme dinleyicisi."""
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.streaming import StreamingQueryListener

KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
DATA = "/data"  # cikti: silver parquet, delta, checkpoint'ler (host'ta ./data)
REF = "/ref"    # statik referans veri (host'ta ./spark/data)

# event_time bilerek String: producer ISO-8601 yaziyor, burada timestamp'e ceviriyoruz
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
         .config("spark.sql.shuffle.partitions", "4")  # lokal makinede 200 partition anlamsiz
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


def read_transactions(spark):
    """Kafka `transactions` -> typed DataFrame. Bozuk (parse edilemeyen) kayitlar atilir.
    Gercek hayatta bunlar bir dead-letter topic'ine yazilir."""
    raw = kafka_stream(spark, "transactions")
    return (raw.select(F.from_json(F.col("value").cast("string"), TXN_SCHEMA).alias("t"))
            .select("t.*")
            .where(F.col("txn_id").isNotNull() & F.col("wallet_id").isNotNull())
            .withColumn("event_time", F.to_timestamp("event_time")))


class ProgressPrinter(StreamingQueryListener):
    """Her micro-batch icin ozet basar. numRowsDroppedByWatermark = watermark yuzunden atilan gec olaylar."""

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
    """Sorgu bitince listener'i temiz kapat. onQueryTerminated olayi JVM'de asenkron dagitildigi icin
    Python callback sunucusu kapanmadan once kisa sure beklenir; yoksa py4j 'Received empty command' hatasi basar."""
    time.sleep(3)
    spark.streams.removeListener(listener)
