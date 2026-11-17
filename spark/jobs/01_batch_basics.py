"""01 - Batch basics: lazy evaluation, join strategies, window functions, skew, partitioning.

Does not need Kafka. While it runs, watch the Spark UI: http://localhost:4040 (open until the job ends).
The "Summary:" comment at the end of each section gives the main idea in one sentence.
"""
from pyspark.sql import Window
from pyspark.sql import functions as F

from common import DATA, REF, get_spark

spark = get_spark("01-batch-basics")
spark.conf.set("spark.sql.adaptive.enabled", "true")


def section(title):
    print("\n" + "=" * 8, title, "=" * 8)


# --- Data: 500K transactions, deliberately SKEWED: W0001 holds ~40% of them
section("Veri uretimi (skew'li)")
wallets = spark.read.option("header", True).csv(f"{REF}/wallets.csv")  # 200 rows, small dimension table
txns = (spark.range(500_000)
        .withColumn("r", F.rand(42))
        .withColumn("wallet_id", F.when(F.col("r") < 0.4, F.lit("W0001"))
                    .otherwise(F.format_string("W%04d", (F.rand(7) * 200 + 1).cast("int"))))
        .withColumn("amount", F.round(F.exp(F.randn(3) + 4.5), 2))
        .withColumn("event_time", F.expr("timestamp'2026-01-01 00:00:00' + make_interval(0,0,0,0,0,0, id / 2.0)"))
        .drop("r"))
txns.groupBy("wallet_id").count().orderBy(F.desc("count")).show(3)
# Summary: "Skew = uneven key distribution; one partition/task does most of the work, so the stage takes as long as its slowest task."

# --- 1) Lazy evaluation + explain
section("Lazy evaluation ve explain()")
q = txns.where(F.col("amount") > 100).groupBy("wallet_id").agg(F.sum("amount").alias("total"))
print("Henuz hicbir sey calismadi (transformation'lar lazy). Plan:")
q.explain()  # Physical plan: HashAggregate -> Exchange (shuffle) -> HashAggregate
print("Action ile tetikleniyor:", q.count(), "grup")
# Summary: "Transformations are lazy, actions trigger execution. A wide transformation (groupBy, join) creates a shuffle = Exchange and splits the stage."

# --- 2) Join strategies
section("Join: broadcast vs sort-merge")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  # turn off automatic broadcast
print(">> Broadcast KAPALI -> SortMergeJoin (iki tarafi da shuffle eder):")
txns.join(wallets, "wallet_id").explain()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))
print(">> Kucuk tabloyu broadcast et -> BroadcastHashJoin (buyuk tarafi shuffle ETMEZ):")
txns.join(F.broadcast(wallets), "wallet_id").explain()
# Summary: "Broadcasting the small dimension table avoids shuffling the large fact table."

# --- 3) Window function
section("Window function: cuzdan basina kosan toplam, sira, onceki islemle sure farki")
w = Window.partitionBy("wallet_id").orderBy("event_time")
(txns.where(F.col("wallet_id").isin("W0010", "W0011"))
 .withColumn("running_total", F.sum("amount").over(w))
 .withColumn("txn_no", F.row_number().over(w))
 .withColumn("secs_since_prev", F.col("event_time").cast("long") - F.lag("event_time").over(w).cast("long"))
 .select("wallet_id", "event_time", "amount", "running_total", "txn_no", "secs_since_prev")
 .show(6, truncate=False))
# Summary: "Velocity/fraud signals can also be computed in batch with window functions; the streaming equivalent is an event-time window."

# --- 4) Handling skew with salting
section("Skew: normal groupBy vs 2 asamali (salted) aggregation")
normal = txns.groupBy("wallet_id").agg(F.sum("amount").alias("total"))
salted = (txns.withColumn("salt", (F.rand(1) * 8).cast("int"))
          .groupBy("wallet_id", "salt").agg(F.sum("amount").alias("partial"))   # stage 1: the hot key is split into 8
          .groupBy("wallet_id").agg(F.sum("partial").alias("total")))           # stage 2: merge the small partial results
a = normal.where("wallet_id = 'W0001'").collect()[0]["total"]
b = salted.where("wallet_id = 'W0001'").collect()[0]["total"]
print("W0001 toplam: normal=%.2f salted=%.2f (ayni sonuc: %s)" % (a, b, abs(a - b) < 0.01))
# Summary: "In Spark 3, AQE splits skewed joins automatically (spark.sql.adaptive.skewJoin.enabled); salting for aggregations is done by hand."

# --- 5) Partition control
section("repartition vs coalesce")
print("baslangic partition sayisi:", txns.rdd.getNumPartitions())
print("repartition(16)  ->", txns.repartition(16).rdd.getNumPartitions(), "(full shuffle, esit dagilim)")
print("coalesce(2)      ->", txns.coalesce(2).rdd.getNumPartitions(), "(shuffle yok, sadece birlestirir)")
# Summary: "Use repartition to increase or balance partitions, coalesce to shrink them before writing."

# --- 6) Parquet + partition pruning
section("Parquet'e partition ile yaz, partition pruning'i gor")
out = f"{DATA}/batch_demo/txns"
(txns.withColumn("txn_date", F.to_date("event_time"))
 .write.mode("overwrite").partitionBy("txn_date").parquet(out))
df = spark.read.parquet(out)
print("Tum partition sayisi (tarih):", df.select("txn_date").distinct().count())
print(">> Filtre partition kolonunda -> plan'da PartitionFilters gorunmeli (sadece ilgili klasor okunur):")
df.where("txn_date = '2026-01-02'").explain()
# Summary: "Parquet is a columnar format; partition pruning + predicate pushdown reduce the data read. Watch out for the small files problem."

spark.stop()
