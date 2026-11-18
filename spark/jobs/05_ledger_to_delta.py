"""05 - Ledger CDC -> Delta Lake: apply the Debezium events of `ledger_entries` to an append-mostly Delta table.

Same pattern as 04 (foreachBatch + idempotent MERGE), keyed by entry_id. Feeds the reconciliation job (06).
Needs `make cdc-up` and `make produce-wallets` (ledger entries only exist with the Postgres source).

Usage:
  make spark JOB=05_ledger_to_delta                         # continuous
  make spark JOB=05_ledger_to_delta ARGS=--available-now    # process what is available and stop
"""
import argparse

from common import DATA, finish, get_spark, kafka_stream, ProgressPrinter
from transforms import apply_ledger_batch, parse_ledger

ap = argparse.ArgumentParser()
ap.add_argument("--available-now", action="store_true")
args = ap.parse_args()

spark = get_spark("05-ledger-to-delta", delta=True)
listener = ProgressPrinter()
spark.streams.addListener(listener)

TABLE_PATH = f"{DATA}/delta/ledger"

ledger = parse_ledger(kafka_stream(spark, "ledger_cdc"))


def upsert(batch_df, batch_id):
    apply_ledger_batch(spark, batch_df, TABLE_PATH)


writer = (ledger.writeStream
          .foreachBatch(upsert)
          .option("checkpointLocation", f"{DATA}/checkpoints/ledger"))
writer = writer.trigger(availableNow=True) if args.available_now else writer.trigger(processingTime="10 seconds")
q = writer.start()
q.awaitTermination()
finish(spark, listener)  # avoids the py4j callback error on exit
