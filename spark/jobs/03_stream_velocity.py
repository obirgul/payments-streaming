"""03 - Fraud velocity: event-time sliding window + watermark -> Kafka `fraud-alerts`.

Rule: alert if the same wallet has >= threshold transactions in a 30 s window (sliding every 10 s).

Concepts: event time vs processing time, sliding window, watermark, late data,
output mode (update vs append), Kafka sink, checkpoint.

Usage:
  make spark JOB=03_stream_velocity                           # update mode, continuous
  make spark JOB=03_stream_velocity ARGS="--mode append"      # writes once when the window closes (watermark passes)
  make spark JOB=03_stream_velocity ARGS=--available-now      # process what is available and stop

NOTE: This job deliberately does not dedup (same rule as the Flink SQL version). Duplicates can inflate the counts;
in production, dedup happens upstream (silver) or this job reads from the silver topic.
"""
import argparse

from pyspark.sql import functions as F

from common import DATA, finish, get_spark, KAFKA, ProgressPrinter, read_transactions

ap = argparse.ArgumentParser()
ap.add_argument("--threshold", type=int, default=6)
ap.add_argument("--mode", choices=["update", "append"], default="update")
ap.add_argument("--sink", choices=["kafka", "console"], default="kafka")
ap.add_argument("--available-now", action="store_true")
args = ap.parse_args()

spark = get_spark("03-stream-velocity")
listener = ProgressPrinter()
spark.streams.addListener(listener)

txns = read_transactions(spark)

agg = (txns
       .withWatermark("event_time", "2 minutes")  # events later than 2 min do not enter the window (dropped)
       .groupBy(F.window("event_time", "30 seconds", "10 seconds"), "wallet_id")
       .agg(F.count("*").alias("txn_count"), F.round(F.sum("amount"), 2).alias("total_amount")))

alerts = agg.where(F.col("txn_count") >= args.threshold)

out = alerts.select(
    F.col("wallet_id").alias("key"),
    F.to_json(F.struct(
        "wallet_id",
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "txn_count", "total_amount",
        F.lit("spark").alias("engine"),
    )).alias("value"))

# Update mode: alert as soon as the window is updated (low latency), but the same window may emit multiple alerts.
# Append mode: ONE result when the window closes via the watermark (at least 2 min delay, no repeats).
if args.sink == "kafka":
    writer = (out.writeStream.format("kafka")
              .option("kafka.bootstrap.servers", KAFKA)
              .option("topic", "fraud-alerts"))
else:
    writer = out.writeStream.format("console").option("truncate", "false")
writer = (writer.outputMode(args.mode)
          .option("checkpointLocation", f"{DATA}/checkpoints/velocity-{args.sink}-{args.mode}"))
writer = writer.trigger(availableNow=True) if args.available_now else writer.trigger(processingTime="5 seconds")

q = writer.start()
q.awaitTermination()
finish(spark, listener)  # avoids the py4j callback error on exit
