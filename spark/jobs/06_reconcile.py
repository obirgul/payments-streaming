"""06 - Balance reconciliation: wallet balance (Delta, from wallet_cdc) vs sum of ledger entries (Delta, from ledger_cdc).

Batch job over the lakehouse. Writes the discrepancies to data/delta/reconciliation (overwritten each run)
and prints a report. Exit code 1 when anything does not reconcile.

What it catches: balance changes that bypassed the ledger (app bugs, manual fixes) AND pipeline problems
(lost or unapplied CDC events on either side).

Caveat: the two Delta tables are fed by independent streams. Run it after both jobs caught up, otherwise
in-flight events show up as transient mismatches.

Usage:
  make spark JOB=06_reconcile
"""
import sys

from pyspark.sql import functions as F

from common import DATA, get_spark
from transforms import reconcile

spark = get_spark("06-reconcile", delta=True)

wallets = spark.read.format("delta").load(f"{DATA}/delta/wallets")
ledger = spark.read.format("delta").load(f"{DATA}/delta/ledger")

issues = reconcile(wallets, ledger).cache()
issues.write.format("delta").mode("overwrite").save(f"{DATA}/delta/reconciliation")

n_wallets, n_entries = wallets.count(), ledger.count()
bad = issues.count()
print("[reconcile] cuzdan=%d, ledger kaydi=%d, uyusmayan=%d" % (n_wallets, n_entries, bad))
for r in issues.groupBy("status").count().collect():
    print("[reconcile]   %s: %d" % (r["status"], r["count"]))
for r in issues.orderBy(F.abs("diff_kurus").desc()).limit(10).collect():
    print("[reconcile]   %s %-16s bakiye=%s ledger=%s fark=%+d kurus" % (
        r["wallet_id"], r["status"], r["balance"], r["ledger_sum"], r["diff_kurus"]))
spark.stop()
sys.exit(1 if bad else 0)
