"""04 - CDC -> Delta Lake (lakehouse): apply Debezium-style events to a table with foreachBatch + MERGE.

Concepts: CDC envelope (before/after/op), "latest record per key" within a micro-batch,
idempotent MERGE (lsn comparison), deletes, ACID table format, foreachBatch.

Usage:
  make spark JOB=04_cdc_to_delta                         # continuous
  make spark JOB=04_cdc_to_delta ARGS=--available-now    # process what is available and stop
"""
import argparse

from common import DATA, finish, get_spark, kafka_stream, ProgressPrinter
from transforms import apply_cdc_batch, parse_cdc

ap = argparse.ArgumentParser()
ap.add_argument("--available-now", action="store_true")
args = ap.parse_args()

spark = get_spark("04-cdc-to-delta", delta=True)
listener = ProgressPrinter()
spark.streams.addListener(listener)

TABLE_PATH = f"{DATA}/delta/wallets"

cdc = parse_cdc(kafka_stream(spark, "wallet_cdc"))


def upsert(batch_df, batch_id):
    apply_cdc_batch(spark, batch_df, TABLE_PATH)  # latest-per-key + idempotent MERGE: see transforms.py


writer = (cdc.writeStream
          .foreachBatch(upsert)
          .option("checkpointLocation", f"{DATA}/checkpoints/cdc"))
writer = writer.queryName("cdc_wallets")  # Prometheus metric name
writer = writer.trigger(availableNow=True) if args.available_now else writer.trigger(processingTime="10 seconds")
q = writer.start()
q.awaitTermination()
finish(spark, listener)  # avoids the py4j callback error on exit
