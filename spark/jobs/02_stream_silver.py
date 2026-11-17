"""02 - Streaming silver katmani: Kafka -> parse -> dedup (watermark) -> zenginlestir -> Parquet.

Kavramlar: Structured Streaming, checkpoint, watermark, dropDuplicatesWithinWatermark,
stream-static join (broadcast), append mode, trigger.

Kullanim:
  make spark JOB=02_stream_silver                         # surekli (10 sn'lik micro-batch)
  make spark JOB=02_stream_silver ARGS=--available-now    # eldeki veriyi isle ve dur (test/backfill)
"""
import argparse

from pyspark.sql import functions as F

from common import DATA, finish, get_spark, ProgressPrinter, read_transactions, REF

ap = argparse.ArgumentParser()
ap.add_argument("--available-now", action="store_true", help="mevcut veriyi isleyip bitir")
args = ap.parse_args()

spark = get_spark("02-stream-silver")
listener = ProgressPrinter()
spark.streams.addListener(listener)

# Statik boyut tablosu (cuzdan segmenti, limit): batch okunur, her micro-batch'te broadcast join yapilir
wallets = (spark.read.option("header", True).csv(f"{REF}/wallets.csv")
           .select("wallet_id", "segment", "city", F.col("daily_limit_try").cast("double")))

txns = read_transactions(spark)

# 1) Dedup: Kafka at-least-once teslimat ve uygulama retry'lari duplicate uretir.
#    State sadece watermark kadar tutulur (5 dk) -> sinirsiz buyumez. 5 dk'dan eski duplicate yakalanmaz (trade-off).
deduped = (txns
           .withWatermark("event_time", "5 minutes")
           .dropDuplicatesWithinWatermark(["txn_id"]))

# 2) Zenginlestirme
silver = (deduped
          .join(F.broadcast(wallets), "wallet_id", "left")
          .withColumn("txn_date", F.to_date("event_time"))
          .withColumn("is_high_value", F.col("amount") >= 2000)
          .withColumn("amount_to_limit", F.round(F.col("amount") / F.col("daily_limit_try"), 4)))

writer = (silver.writeStream
          .format("parquet")
          .option("path", f"{DATA}/silver/transactions")
          .option("checkpointLocation", f"{DATA}/checkpoints/silver")  # offset + state burada: yeniden baslayinca kaldigi yerden devam
          .partitionBy("txn_date")
          .outputMode("append"))
writer = writer.trigger(availableNow=True) if args.available_now else writer.trigger(processingTime="10 seconds")

q = writer.start()
q.awaitTermination()
finish(spark, listener)  # cikista py4j callback hatasi gormemek icin
