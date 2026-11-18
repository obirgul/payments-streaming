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

from common import DATA, finish, get_spark, KAFKA, ProgressPrinter, read_transactions
from transforms import alerts_to_kafka, velocity_alerts

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

# Window/watermark/threshold logic lives in transforms.py (unit tested).
# Events later than 2 min do not enter the window (dropped).
out = alerts_to_kafka(velocity_alerts(txns, args.threshold))

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
