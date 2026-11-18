"""Pure DataFrame transformations used by the Spark jobs.

They live here (not inside the job scripts) so they can be unit tested with a batch SparkSession,
without Kafka. The job scripts only wire sources and sinks around these functions.
"""
from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

# ---------------------------------------------------------------- 02 silver
HIGH_VALUE_TRY = 2000


def dedup(txns):
    """Dedup by txn_id within a 5 min watermark. Streaming only (dropDuplicatesWithinWatermark has no batch mode)."""
    return txns.withWatermark("event_time", "5 minutes").dropDuplicatesWithinWatermark(["txn_id"])


def enrich(txns, wallets):
    """Wallet dimension (broadcast left join) + derived columns."""
    return (txns
            .join(F.broadcast(wallets), "wallet_id", "left")
            .withColumn("txn_date", F.to_date("event_time"))
            .withColumn("is_high_value", F.col("amount") >= HIGH_VALUE_TRY)
            .withColumn("amount_to_limit", F.round(F.col("amount") / F.col("daily_limit_try"), 4)))


# ---------------------------------------------------------------- 03 velocity
# Keep in sync with flink/sql/fraud_velocity.sql: 30 s window, 10 s slide, threshold 6.
WINDOW = "30 seconds"
SLIDE = "10 seconds"
WATERMARK = "2 minutes"
DEFAULT_THRESHOLD = 6


def velocity_alerts(txns, threshold=DEFAULT_THRESHOLD):
    """Wallets with >= threshold transactions in a sliding event-time window."""
    agg = (txns
           .withWatermark("event_time", WATERMARK)
           .groupBy(F.window("event_time", WINDOW, SLIDE), "wallet_id")
           .agg(F.count("*").alias("txn_count"), F.round(F.sum("amount"), 2).alias("total_amount")))
    return agg.where(F.col("txn_count") >= threshold)


def alerts_to_kafka(alerts):
    """Alerts -> (key, value) columns for the Kafka sink."""
    return alerts.select(
        F.col("wallet_id").alias("key"),
        F.to_json(F.struct(
            "wallet_id",
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "txn_count", "total_amount",
            F.lit("spark").alias("engine"),
        )).alias("value"))


# ---------------------------------------------------------------- 04 cdc
ROW = T.StructType([
    T.StructField("wallet_id", T.StringType()),
    T.StructField("user_id", T.StringType()),
    T.StructField("balance_kurus", T.LongType()),
    T.StructField("status", T.StringType()),
])
CDC_SCHEMA = T.StructType([
    T.StructField("op", T.StringType()),  # r=snapshot read, c=create, u=update, d=delete (Debezium)
    T.StructField("ts_ms", T.LongType()),
    T.StructField("source", T.StructType([T.StructField("lsn", T.LongType())])),
    T.StructField("before", ROW),
    T.StructField("after", ROW),
])
CDC_COLS = ["wallet_id", "user_id", "balance_kurus", "status", "lsn", "ts_ms"]


def parse_cdc(raw):
    """Frame with a `value` column (Debezium JSON envelope) -> flat columns (op, lsn, ts_ms + wallet columns).
    Records without a wallet key (tombstones = null value, unparseable JSON) are dropped.
    Extra Debezium fields (source.txId, snapshot, updated_at ...) are ignored by the schema."""
    return (raw
            .select(F.from_json(F.col("value").cast("string"), CDC_SCHEMA).alias("c"))
            .select("c.*")
            .select(
                "op",
                F.col("source.lsn").alias("lsn"),
                "ts_ms",
                # on delete `after` is empty, the key is in `before`
                F.coalesce(F.col("after.wallet_id"), F.col("before.wallet_id")).alias("wallet_id"),
                F.col("after.user_id").alias("user_id"),
                F.col("after.balance_kurus").alias("balance_kurus"),
                F.col("after.status").alias("status"),
            )
            .where(F.col("wallet_id").isNotNull()))


def latest_per_wallet(batch_df):
    """A micro-batch can hold several events for one wallet -> keep only the latest (highest lsn).
    (MERGE fails if more than one source row matches the same key.)"""
    w = Window.partitionBy("wallet_id").orderBy(F.col("lsn").desc())
    return batch_df.withColumn("rn", F.row_number().over(w)).where("rn = 1").drop("rn")


def apply_cdc_batch(spark, batch_df, table_path):
    """Idempotent MERGE of one micro-batch into the Delta wallets table."""
    latest = latest_per_wallet(batch_df)

    if not DeltaTable.isDeltaTable(spark, table_path):
        latest.where("1 = 0").select(*CDC_COLS).write.format("delta").save(table_path)  # empty table + schema

    target = DeltaTable.forPath(spark, table_path)
    (target.alias("t")
     .merge(latest.alias("s"), "t.wallet_id = s.wallet_id")
     # `s.lsn > t.lsn`: an old/replayed event does not overwrite newer state -> MERGE is idempotent
     .whenMatchedDelete(condition="s.op = 'd' AND s.lsn > t.lsn")
     .whenMatchedUpdate(condition="s.op != 'd' AND s.lsn > t.lsn", set={c: f"s.{c}" for c in CDC_COLS})
     .whenNotMatchedInsert(condition="s.op != 'd'", values={c: f"s.{c}" for c in CDC_COLS})
     .execute())


# ---------------------------------------------------------------- 05 ledger
LEDGER_ROW = T.StructType([
    T.StructField("entry_id", T.LongType()),
    T.StructField("wallet_id", T.StringType()),
    T.StructField("delta_kurus", T.LongType()),
    T.StructField("reason", T.StringType()),
])
LEDGER_CDC_SCHEMA = T.StructType([
    T.StructField("op", T.StringType()),
    T.StructField("ts_ms", T.LongType()),
    T.StructField("source", T.StructType([T.StructField("lsn", T.LongType())])),
    T.StructField("before", LEDGER_ROW),
    T.StructField("after", LEDGER_ROW),
])
LEDGER_COLS = ["entry_id", "wallet_id", "delta_kurus", "reason", "lsn", "ts_ms"]


def parse_ledger(raw):
    """Debezium envelope of ledger_entries -> flat columns. Entries without a key (tombstones, garbage) are dropped."""
    return (raw
            .select(F.from_json(F.col("value").cast("string"), LEDGER_CDC_SCHEMA).alias("c"))
            .select("c.*")
            .select(
                "op",
                F.col("source.lsn").alias("lsn"),
                "ts_ms",
                F.coalesce(F.col("after.entry_id"), F.col("before.entry_id")).alias("entry_id"),
                F.coalesce(F.col("after.wallet_id"), F.col("before.wallet_id")).alias("wallet_id"),
                F.col("after.delta_kurus").alias("delta_kurus"),
                F.col("after.reason").alias("reason"),
            )
            .where(F.col("entry_id").isNotNull()))


def apply_ledger_batch(spark, batch_df, table_path):
    """Idempotent MERGE of ledger events keyed by entry_id. The ledger is append-only in normal operation;
    deletes only happen when a test run wipes the table, and are applied so stale entries do not cause false mismatches."""
    w = Window.partitionBy("entry_id").orderBy(F.col("lsn").desc())
    latest = batch_df.withColumn("rn", F.row_number().over(w)).where("rn = 1").drop("rn")

    if not DeltaTable.isDeltaTable(spark, table_path):
        latest.where("1 = 0").select(*LEDGER_COLS).write.format("delta").save(table_path)

    (DeltaTable.forPath(spark, table_path).alias("t")
     .merge(latest.alias("s"), "t.entry_id = s.entry_id")
     .whenMatchedDelete(condition="s.op = 'd' AND s.lsn > t.lsn")
     .whenMatchedUpdate(condition="s.op != 'd' AND s.lsn > t.lsn", set={c: f"s.{c}" for c in LEDGER_COLS})
     .whenNotMatchedInsert(condition="s.op != 'd'", values={c: f"s.{c}" for c in LEDGER_COLS})
     .execute())


# ---------------------------------------------------------------- 06 reconciliation
def reconcile(wallets, ledger):
    """Compares each wallet balance with the sum of its ledger entries. Returns ONLY the discrepancies:

      balance_mismatch  wallet exists, ledger exists, balance != ledger sum
      missing_ledger    wallet has a non-zero balance but no ledger entries at all
      orphan_ledger     ledger entries remain for a wallet that no longer exists and they do not net to zero
                        (a closed wallet must have been paid out to exactly zero)

    `diff_kurus` = balance - ledger_sum (positive: money in the balance the ledger cannot explain)."""
    sums = ledger.groupBy("wallet_id").agg(F.sum("delta_kurus").alias("ledger_sum"), F.count("*").alias("entries"))
    joined = (wallets.select("wallet_id", F.col("balance_kurus").alias("balance"))
              .join(sums, "wallet_id", "full_outer"))
    balance = F.coalesce(F.col("balance"), F.lit(0))
    ledger_sum = F.coalesce(F.col("ledger_sum"), F.lit(0))
    status = (F.when(F.col("balance").isNull(), F.lit("orphan_ledger"))
              .when(F.col("entries").isNull(), F.lit("missing_ledger"))
              .otherwise(F.lit("balance_mismatch")))
    return (joined
            .withColumn("diff_kurus", balance - ledger_sum)
            .where(F.col("diff_kurus") != 0)
            .withColumn("status", status)
            .select("wallet_id", "status", "balance", "ledger_sum", "entries", "diff_kurus"))
