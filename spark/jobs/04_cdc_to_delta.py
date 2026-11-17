"""04 - CDC -> Delta Lake (lakehouse): Debezium bicimli olaylari foreachBatch + MERGE ile tabloya uygula.

Kavramlar: CDC zarfi (before/after/op), micro-batch icinde "ayni anahtarin en son kaydi",
idempotent MERGE (lsn karsilastirmasi), silme (delete), ACID tablo formati, foreachBatch.

Kullanim:
  make spark JOB=04_cdc_to_delta                         # surekli
  make spark JOB=04_cdc_to_delta ARGS=--available-now    # eldeki veriyi isle ve dur
"""
import argparse

from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from common import DATA, ProgressPrinter, get_spark, kafka_stream

ap = argparse.ArgumentParser()
ap.add_argument("--available-now", action="store_true")
args = ap.parse_args()

spark = get_spark("04-cdc-to-delta", delta=True)
listener = ProgressPrinter()
spark.streams.addListener(listener)

TABLE_PATH = f"{DATA}/delta/wallets"

ROW = T.StructType([
    T.StructField("wallet_id", T.StringType()),
    T.StructField("user_id", T.StringType()),
    T.StructField("balance_kurus", T.LongType()),
    T.StructField("status", T.StringType()),
])
CDC_SCHEMA = T.StructType([
    T.StructField("op", T.StringType()),  # c=create, u=update, d=delete
    T.StructField("ts_ms", T.LongType()),
    T.StructField("source", T.StructType([T.StructField("lsn", T.LongType())])),
    T.StructField("before", ROW),
    T.StructField("after", ROW),
])

cdc = (kafka_stream(spark, "wallet_cdc")
       .select(F.from_json(F.col("value").cast("string"), CDC_SCHEMA).alias("c"))
       .select("c.*")
       .select(
           "op",
           F.col("source.lsn").alias("lsn"),
           "ts_ms",
           # delete'te `after` bos, anahtar `before` icinde
           F.coalesce(F.col("after.wallet_id"), F.col("before.wallet_id")).alias("wallet_id"),
           F.col("after.user_id").alias("user_id"),
           F.col("after.balance_kurus").alias("balance_kurus"),
           F.col("after.status").alias("status"),
       ))

COLS = ["wallet_id", "user_id", "balance_kurus", "status", "lsn", "ts_ms"]


def upsert(batch_df, batch_id):
    # 1) Ayni micro-batch'te bir cuzdan icin birden cok olay olabilir -> sadece en sonuncusu (en buyuk lsn) uygulanir.
    #    (MERGE, kaynakta ayni anahtardan birden fazla satir eslesirse hata verir.)
    w = Window.partitionBy("wallet_id").orderBy(F.col("lsn").desc())
    latest = batch_df.withColumn("rn", F.row_number().over(w)).where("rn = 1").drop("rn")

    if not DeltaTable.isDeltaTable(spark, TABLE_PATH):
        (latest.where("1 = 0").select(*COLS).write.format("delta").save(TABLE_PATH))  # bos tablo + sema

    target = DeltaTable.forPath(spark, TABLE_PATH)
    (target.alias("t")
     .merge(latest.alias("s"), "t.wallet_id = s.wallet_id")
     # `s.lsn > t.lsn`: eski/tekrar oynatilan olay yeni durumu ezmez -> MERGE idempotent
     .whenMatchedDelete(condition="s.op = 'd' AND s.lsn > t.lsn")
     .whenMatchedUpdate(condition="s.op != 'd' AND s.lsn > t.lsn", set={c: f"s.{c}" for c in COLS})
     .whenNotMatchedInsert(condition="s.op != 'd'", values={c: f"s.{c}" for c in COLS})
     .execute())


writer = (cdc.writeStream
          .foreachBatch(upsert)
          .option("checkpointLocation", f"{DATA}/checkpoints/cdc"))
writer = writer.trigger(availableNow=True) if args.available_now else writer.trigger(processingTime="10 seconds")
q = writer.start()
q.awaitTermination()
spark.streams.removeListener(listener)  # cikista py4j callback hatasi gormemek icin
