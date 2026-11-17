"""01 - Batch temelleri: lazy evaluation, join stratejileri, window function, skew, partition.

Kafka gerektirmez. Calistirirken Spark UI'a bak: http://localhost:4040 (is bitene kadar acik).
Her bolumun sonunda "Ozet:" yorumu ana fikri tek cumlede verir.
"""
from pyspark.sql import Window
from pyspark.sql import functions as F

from common import DATA, REF, get_spark

spark = get_spark("01-batch-basics")
spark.conf.set("spark.sql.adaptive.enabled", "true")


def section(title):
    print("\n" + "=" * 8, title, "=" * 8)


# --- Veri: 500K islem, kasitli olarak CARPIK (skewed): W0001 islemlerin ~%40'i
section("Veri uretimi (skew'li)")
wallets = spark.read.option("header", True).csv(f"{REF}/wallets.csv")  # 200 satir, kucuk boyut tablosu
txns = (spark.range(500_000)
        .withColumn("r", F.rand(42))
        .withColumn("wallet_id", F.when(F.col("r") < 0.4, F.lit("W0001"))
                    .otherwise(F.format_string("W%04d", (F.rand(7) * 200 + 1).cast("int"))))
        .withColumn("amount", F.round(F.exp(F.randn(3) + 4.5), 2))
        .withColumn("event_time", F.expr("timestamp'2026-01-01 00:00:00' + make_interval(0,0,0,0,0,0, id / 2.0)"))
        .drop("r"))
txns.groupBy("wallet_id").count().orderBy(F.desc("count")).show(3)
# Ozet: "Skew = anahtar dagiliminin esit olmamasi; tek partition/task cok is yapar, stage en yavas task kadar uzar."

# --- 1) Lazy evaluation + explain
section("Lazy evaluation ve explain()")
q = txns.where(F.col("amount") > 100).groupBy("wallet_id").agg(F.sum("amount").alias("total"))
print("Henuz hicbir sey calismadi (transformation'lar lazy). Plan:")
q.explain()  # Physical plan: HashAggregate -> Exchange (shuffle) -> HashAggregate
print("Action ile tetikleniyor:", q.count(), "grup")
# Ozet: "Transformation lazy, action tetikler. Wide transformation (groupBy, join) shuffle = Exchange yaratir ve stage'i boler."

# --- 2) Join stratejileri
section("Join: broadcast vs sort-merge")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  # otomatik broadcast'i kapat
print(">> Broadcast KAPALI -> SortMergeJoin (iki tarafi da shuffle eder):")
txns.join(wallets, "wallet_id").explain()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))
print(">> Kucuk tabloyu broadcast et -> BroadcastHashJoin (buyuk tarafi shuffle ETMEZ):")
txns.join(F.broadcast(wallets), "wallet_id").explain()
# Ozet: "Kucuk boyut tablosunu broadcast ederek buyuk fact tablosunun shuffle'ini onlerim."

# --- 3) Window function
section("Window function: cuzdan basina kosan toplam, sira, onceki islemle sure farki")
w = Window.partitionBy("wallet_id").orderBy("event_time")
(txns.where(F.col("wallet_id").isin("W0010", "W0011"))
 .withColumn("running_total", F.sum("amount").over(w))
 .withColumn("txn_no", F.row_number().over(w))
 .withColumn("secs_since_prev", F.col("event_time").cast("long") - F.lag("event_time").over(w).cast("long"))
 .select("wallet_id", "event_time", "amount", "running_total", "txn_no", "secs_since_prev")
 .show(6, truncate=False))
# Ozet: "Velocity/fraud sinyalleri window function ile batch'te de hesaplanir; streaming'de karsiligi event-time window'dur."

# --- 4) Skew'i salting ile yonetme
section("Skew: normal groupBy vs 2 asamali (salted) aggregation")
normal = txns.groupBy("wallet_id").agg(F.sum("amount").alias("total"))
salted = (txns.withColumn("salt", (F.rand(1) * 8).cast("int"))
          .groupBy("wallet_id", "salt").agg(F.sum("amount").alias("partial"))   # 1. asama: sicak anahtar 8'e bolunur
          .groupBy("wallet_id").agg(F.sum("partial").alias("total")))           # 2. asama: kucuk veriyi birlestir
a = normal.where("wallet_id = 'W0001'").collect()[0]["total"]
b = salted.where("wallet_id = 'W0001'").collect()[0]["total"]
print("W0001 toplam: normal=%.2f salted=%.2f (ayni sonuc: %s)" % (a, b, abs(a - b) < 0.01))
# Ozet: "Spark 3'te AQE skew join'i otomatik boler (spark.sql.adaptive.skewJoin.enabled); aggregation'da salting elle yapilir."

# --- 5) Partition kontrolu
section("repartition vs coalesce")
print("baslangic partition sayisi:", txns.rdd.getNumPartitions())
print("repartition(16)  ->", txns.repartition(16).rdd.getNumPartitions(), "(full shuffle, esit dagilim)")
print("coalesce(2)      ->", txns.coalesce(2).rdd.getNumPartitions(), "(shuffle yok, sadece birlestirir)")
# Ozet: "Artirmak/dengelemek icin repartition, yazmadan once kucultmek icin coalesce."

# --- 6) Parquet + partition pruning
section("Parquet'e partition ile yaz, partition pruning'i gor")
out = f"{DATA}/batch_demo/txns"
(txns.withColumn("txn_date", F.to_date("event_time"))
 .write.mode("overwrite").partitionBy("txn_date").parquet(out))
df = spark.read.parquet(out)
print("Tum partition sayisi (tarih):", df.select("txn_date").distinct().count())
print(">> Filtre partition kolonunda -> plan'da PartitionFilters gorunmeli (sadece ilgili klasor okunur):")
df.where("txn_date = '2026-01-02'").explain()
# Ozet: "Parquet kolonlu formattir; partition pruning + predicate pushdown okunan veriyi azaltir. Cok kucuk dosya (small files) problemine dikkat."

spark.stop()
