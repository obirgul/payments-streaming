"""02 - Streaming silver layer: Kafka -> parse -> dedup (watermark) -> enrich -> Parquet.

Concepts: Structured Streaming, checkpoint, watermark, dropDuplicatesWithinWatermark,
stream-static join (broadcast), append mode, trigger.

Usage:
  make spark JOB=02_stream_silver                         # continuous (10 s micro-batches)
  make spark JOB=02_stream_silver ARGS=--available-now    # process what is available and stop (test/backfill)
"""
import argparse

from pyspark.sql import functions as F

from common import DATA, finish, get_spark, ProgressPrinter, read_transactions, REF
from transforms import dedup, enrich

ap = argparse.ArgumentParser()
ap.add_argument("--available-now", action="store_true", help="mevcut veriyi isleyip bitir")
args = ap.parse_args()

spark = get_spark("02-stream-silver")
listener = ProgressPrinter()
spark.streams.addListener(listener)

# Static dimension table (wallet segment, limit): read as batch, broadcast-joined in every micro-batch
wallets = (spark.read.option("header", True).csv(f"{REF}/wallets.csv")
           .select("wallet_id", "segment", "city", F.col("daily_limit_try").cast("double")))

txns = read_transactions(spark)

# Dedup (Kafka at-least-once delivery and application retries produce duplicates) + enrichment: see transforms.py.
# Dedup state is kept only for the watermark duration (5 min), so it does not grow unbounded.
# Duplicates older than 5 min are not caught (trade-off).
silver = enrich(dedup(txns), wallets)

writer = (silver.writeStream
          .format("parquet")
          .option("path", f"{DATA}/silver/transactions")
          .option("checkpointLocation", f"{DATA}/checkpoints/silver")  # offsets + state live here: on restart the job resumes where it left off
          .partitionBy("txn_date")
          .outputMode("append"))
writer = writer.trigger(availableNow=True) if args.available_now else writer.trigger(processingTime="10 seconds")

q = writer.start()
q.awaitTermination()
finish(spark, listener)  # avoids the py4j callback error on exit
