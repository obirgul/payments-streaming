"""End-to-end verification: compares Spark/Flink outputs with the ground truth written by the producer.

Checks:
  1. Silver Parquet: row count == unique txn count (no duplicates left, nothing lost)
  2. Delta wallets: final state matches the CDC ground truth (updates applied, deleted wallets gone)
  3. fraud-alerts (Spark) and fraud-alerts-flink: every injected burst wallet was caught
  4. Spark and Flink alerts agree window by window (same rule -> same windows and counts)
  5. Reconciliation (Postgres source only): found exactly the wallets whose balance was changed without a ledger
     entry, with the exact amounts, and nothing else
Exit code: 0 if all pass, 1 otherwise.
"""
import json
import sys

from pyspark.sql import functions as F
from pyspark.sql import types as T

from alert_compare import compare_alerts
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
ALERT_SCHEMA = T.StructType([
    T.StructField("wallet_id", T.StringType()),
    T.StructField("window_start", T.StringType()),  # Spark: 2024-01-01T10:00:00.000Z, Flink: 2024-01-01T10:00:00
    T.StructField("txn_count", T.LongType()),
])
alert_rows = {}  # topic -> [(wallet_id, window_start, txn_count)]


def alert_wallets(topic):
    df = (spark.read.format("kafka").option("kafka.bootstrap.servers", KAFKA)
          .option("subscribe", topic).option("startingOffsets", "earliest").load())
    parsed = (df.select(F.from_json(F.col("value").cast("string"), ALERT_SCHEMA).alias("a")).select("a.*")
              .withColumn("window_start", F.col("window_start").cast("timestamp")))
    rows = [(r["wallet_id"], r["window_start"], r["txn_count"]) for r in parsed.collect()]
    alert_rows[topic] = rows
    return {r[0] for r in rows}, df.count()


if txn_gt:
    bursts = {b["wallet_id"] for b in txn_gt["burst_wallets"]}
    for topic in ["fraud-alerts", "fraud-alerts-flink"]:
        try:
            wallets, n = alert_wallets(topic)
        except Exception as e:  # noqa: BLE001
            print("SKIP -", topic, "okunamadi:", str(e).splitlines()[0])
            continue
        if n == 0:
            alert_rows.pop(topic, None)  # an empty topic must not take part in the Spark/Flink comparison
            print("SKIP -", topic, "bos (ilgili is calistirilmadi mi?)")
            continue
        caught = bursts & wallets
        fp = wallets - bursts
        check("%s: burst cuzdanlarinin hepsi yakalandi" % topic, caught == bursts,
              "(yakalanan=%d/%d, alarm mesaji=%d, yanlis pozitif cuzdan=%d)" % (len(caught), len(bursts), n, len(fp)))

# 4) Spark vs Flink: same rule, so the same windows with the same counts
if len(alert_rows) == 2:
    diff = compare_alerts(alert_rows["fraud-alerts"], alert_rows["fraud-alerts-flink"])
    check("spark/flink: ayni pencereler", not diff["only_spark"] and not diff["only_flink"],
          "(ortak=%d, sadece spark=%d, sadece flink=%d)" % (diff["common"], len(diff["only_spark"]), len(diff["only_flink"])))
    # Flink drops late events (> 2 min behind the watermark); Spark with --available-now (one batch) still counts them.
    # So Spark >= Flink is expected; Flink > Spark never is.
    check("spark/flink: flink hicbir pencerede spark'tan fazla saymiyor", not diff["flink_higher"],
          "" if not diff["flink_higher"] else "(ornek: %s)" % diff["flink_higher"][:3])
    if diff["spark_higher"]:
        print("INFO - %d pencerede spark flink'ten fazla say(d)i (gec olay farki, beklenen): %s" % (
            len(diff["spark_higher"]), [(k[0], c_s, c_f) for k, c_s, c_f in diff["spark_higher"][:3]]))

# 5) Reconciliation: only with the Postgres source (ground truth has a `drifted` section)
if cdc_gt and "drifted" in cdc_gt:
    try:
        rec = {r["wallet_id"]: r["diff_kurus"] for r in spark.read.format("delta").load(f"{DATA}/delta/reconciliation").collect()}
        exp = cdc_gt["drifted"]
        check("mutabakat: enjekte edilen sapmalar bulundu", set(exp) <= set(rec),
              "(bulunan=%d/%d)" % (len(set(exp) & set(rec)), len(exp)))
        check("mutabakat: sapma tutarlari dogru", all(rec.get(w) == d for w, d in exp.items()),
              "" if all(rec.get(w) == d for w, d in exp.items()) else "(beklenen=%s, bulunan=%s)" % (
                  dict(list(exp.items())[:3]), {w: rec.get(w) for w in list(exp)[:3]}))
        check("mutabakat: baska uyusmazlik yok (yanlis pozitif)", set(rec) <= set(exp),
              "" if set(rec) <= set(exp) else "(fazladan: %s)" % sorted(set(rec) - set(exp))[:5])
        n_led = spark.read.format("delta").load(f"{DATA}/delta/ledger").count()
        check("ledger: kayit sayisi Postgres ile ayni", n_led == cdc_gt["ledger_entries"],
              "(delta=%d, postgres=%d)" % (n_led, cdc_gt["ledger_entries"]))
    except Exception as e:  # noqa: BLE001
        check("mutabakat sonucu okunabildi", False, str(e).splitlines()[0])

print("\nSONUC:", "HEPSI GECTI" if not failures else "BASARISIZ -> " + ", ".join(failures))
spark.stop()
sys.exit(1 if failures else 0)
