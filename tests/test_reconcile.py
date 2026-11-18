import shutil
import tempfile

from spark_base import SparkTestCase
from test_cdc import row
from transforms import apply_ledger_batch, parse_ledger, reconcile


def lrow(entry_id, wallet_id, delta, reason="adjustment"):
    return {"entry_id": entry_id, "wallet_id": wallet_id, "delta_kurus": delta, "reason": reason}


def levent(op, lsn, before=None, after=None):
    return {"op": op, "ts_ms": 1000 + lsn, "source": {"lsn": lsn}, "before": before, "after": after}


class ReconcileTest(SparkTestCase):
    def run_rec(self, balances, entries):
        wallets = self.spark.createDataFrame([(w, b) for w, b in balances.items()], "wallet_id string, balance_kurus long")
        ledger = self.spark.createDataFrame([(i, w, d) for i, (w, d) in enumerate(entries)],
                                            "entry_id long, wallet_id string, delta_kurus long")
        return {r["wallet_id"]: r for r in reconcile(wallets, ledger).collect()}

    def test_matching_wallets_produce_nothing(self):
        out = self.run_rec({"W1": 150, "W2": 0}, [("W1", 100), ("W1", 50), ("W2", 70), ("W2", -70)])
        self.assertEqual(out, {})

    def test_balance_mismatch_reports_signed_diff(self):
        out = self.run_rec({"W1": 160, "W2": 40}, [("W1", 100), ("W1", 50), ("W2", 50)])
        self.assertEqual((out["W1"]["status"], out["W1"]["diff_kurus"]), ("balance_mismatch", 10))
        self.assertEqual((out["W2"]["status"], out["W2"]["diff_kurus"]), ("balance_mismatch", -10))

    def test_wallet_with_balance_but_no_ledger(self):
        out = self.run_rec({"W1": 500}, [])
        self.assertEqual((out["W1"]["status"], out["W1"]["diff_kurus"]), ("missing_ledger", 500))

    def test_empty_wallet_without_ledger_is_fine(self):
        self.assertEqual(self.run_rec({"W1": 0}, []), {})

    def test_closed_wallet_netting_to_zero_is_fine(self):
        # wallet row deleted, ledger kept: opening +100, close -100
        self.assertEqual(self.run_rec({}, [("W9", 100), ("W9", -100)]), {})

    def test_closed_wallet_with_leftover_ledger_is_orphan(self):
        out = self.run_rec({}, [("W9", 100), ("W9", -60)])
        self.assertEqual((out["W9"]["status"], out["W9"]["diff_kurus"]), ("orphan_ledger", -40))

    def test_only_bad_wallets_are_returned(self):
        out = self.run_rec({"W1": 10, "W2": 20, "W3": 30}, [("W1", 10), ("W2", 25), ("W3", 30)])
        self.assertEqual(set(out), {"W2"})


class LedgerCdcTest(SparkTestCase):
    def setUp(self):
        self.path = tempfile.mkdtemp(prefix="ledger_delta_")
        self.addCleanup(shutil.rmtree, self.path, True)

    def apply(self, *events):
        apply_ledger_batch(self.spark, parse_ledger(self.json_df(list(events))), self.path)

    def entries(self):
        return {r["entry_id"]: r["delta_kurus"] for r in self.spark.read.format("delta").load(self.path).collect()}

    def test_parse_drops_tombstones_and_keeps_delete_key(self):
        rows = parse_ledger(self.json_df(["null", levent("d", 3, before=lrow(7, "W1", 5))])).collect()
        self.assertEqual([(r["op"], r["entry_id"], r["wallet_id"]) for r in rows], [("d", 7, "W1")])

    def test_insert_and_replay_is_idempotent(self):
        batch = [levent("c", 1, after=lrow(1, "W1", 100)), levent("c", 2, after=lrow(2, "W1", -30))]
        self.apply(*batch)
        self.apply(*batch)
        self.assertEqual(self.entries(), {1: 100, 2: -30})

    def test_delete_removes_entry(self):
        self.apply(levent("c", 1, after=lrow(1, "W1", 100)), levent("c", 2, after=lrow(2, "W1", 5)))
        self.apply(levent("d", 3, before=lrow(2, "W1", 5)))
        self.assertEqual(self.entries(), {1: 100})

    def test_end_to_end_with_wallet_state(self):
        """Ledger sums feed reconcile exactly like the job does."""
        self.apply(levent("c", 1, after=lrow(1, "W1", 100)), levent("c", 2, after=lrow(2, "W1", 50)))
        ledger = self.spark.read.format("delta").load(self.path)
        wallets = self.spark.createDataFrame([("W1", 160)], "wallet_id string, balance_kurus long")
        out = reconcile(wallets, ledger).collect()
        self.assertEqual([(r["wallet_id"], r["diff_kurus"]) for r in out], [("W1", 10)])
