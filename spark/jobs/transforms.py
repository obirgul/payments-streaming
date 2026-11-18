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
