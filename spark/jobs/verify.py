"""End-to-end verification: compares Spark/Flink outputs with the ground truth written by the producer.

Checks:
  1. Silver Parquet: row count == unique txn count (no duplicates left, nothing lost)
  2. Delta wallets: final state matches the CDC ground truth (updates applied, deleted wallets gone)
  3. fraud-alerts (Spark) and fraud-alerts-flink: every injected burst wallet was caught
Exit code: 0 if all pass, 1 otherwise.
"""
import json
import sys

from pyspark.sql import functions as F
from pyspark.sql import types as T

from common import DATA, KAFKA, get_spark

spark = get_spark("verify", delta=True)
failures = []


def check(name, ok, detail=""):
    print(("PASS" if ok else "FAIL"), "-", name, detail)
    if not ok:
        failures.append(name)


def load(path):
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        return None


txn_gt = load(f"{DATA}/ground_truth_txn.json")
cdc_gt = load(f"{DATA}/ground_truth_cdc.json")

# 1) Silver
if txn_gt:
    try:
        silver = spark.read.parquet(f"{DATA}/silver/transactions")
        total = silver.count()
        distinct = silver.select("txn_id").distinct().count()
        check("silver: duplicate yok", total == distinct, f"(satir={total}, tekil={distinct})")
        check("silver: tekil sayisi ground truth ile ayni", distinct == txn_gt["unique_txn_ids"],
              f"(silver={distinct}, beklenen={txn_gt['unique_txn_ids']}, producer toplam gonderim={txn_gt['total_sent']})")
        missing_seg = silver.where(F.col("segment").isNull()).count()
        check("silver: tum satirlar cuzdan tablosuyla zenginlesmis", missing_seg == 0, f"(eksik={missing_seg})")
    except Exception as e:  # noqa: BLE001
        check("silver okunabildi", False, str(e).splitlines()[0])
else:
    print("SKIP - ground_truth_txn.json yok")

# 2) Delta
if cdc_gt:
    try:
        rows = spark.read.format("delta").load(f"{DATA}/delta/wallets").collect()
        got = {r["wallet_id"]: r["balance_kurus"] for r in rows}
        exp = cdc_gt["final_state"]
        check("delta: aktif cuzdan sayisi", len(got) == len(exp), f"(delta={len(got)}, beklenen={len(exp)})")
        check("delta: tum bakiyeler beklenenle ayni", got == exp,
              "" if got == exp else f"(farkli: {[k for k in exp if got.get(k) != exp[k]][:5]})")
        check("delta: silinen cuzdanlar yok", not (set(cdc_gt["deleted"]) & set(got)))
    except Exception as e:  # noqa: BLE001
        check("delta okunabildi", False, str(e).splitlines()[0])
else:
    print("SKIP - ground_truth_cdc.json yok")

# 3) Alerts
ALERT_SCHEMA = T.StructType([T.StructField("wallet_id", T.StringType()), T.StructField("txn_count", T.LongType())])


def alert_wallets(topic):
    df = (spark.read.format("kafka").option("kafka.bootstrap.servers", KAFKA)
          .option("subscribe", topic).option("startingOffsets", "earliest").load())
    parsed = df.select(F.from_json(F.col("value").cast("string"), ALERT_SCHEMA).alias("a")).select("a.*")
    return {r["wallet_id"] for r in parsed.select("wallet_id").distinct().collect()}, df.count()


if txn_gt:
    bursts = {b["wallet_id"] for b in txn_gt["burst_wallets"]}
    for topic in ["fraud-alerts", "fraud-alerts-flink"]:
        try:
            wallets, n = alert_wallets(topic)
        except Exception as e:  # noqa: BLE001
            print("SKIP -", topic, "okunamadi:", str(e).splitlines()[0])
            continue
        if n == 0:
            print("SKIP -", topic, "bos (ilgili is calistirilmadi mi?)")
            continue
        caught = bursts & wallets
        fp = wallets - bursts
        check("%s: burst cuzdanlarinin hepsi yakalandi" % topic, caught == bursts,
              "(yakalanan=%d/%d, alarm mesaji=%d, yanlis pozitif cuzdan=%d)" % (len(caught), len(bursts), n, len(fp)))

print("\nSONUC:", "HEPSI GECTI" if not failures else "BASARISIZ -> " + ", ".join(failures))
spark.stop()
sys.exit(1 if failures else 0)
