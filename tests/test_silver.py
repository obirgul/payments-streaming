from spark_base import SparkTestCase, stream_collect, txn
from transforms import dedup, enrich


class SilverTest(SparkTestCase):
    def wallets(self):
        return self.spark.createDataFrame(
            [("W0001", "premium", "Ankara", 20000.0), ("W0002", "new", "Izmir", 1000.0)],
            ["wallet_id", "segment", "city", "daily_limit_try"])

    def run_silver(self, events):
        return {r["txn_id"]: r for r in enrich(self.txns(events), self.wallets()).collect()}

    def test_duplicates_are_removed(self):
        # real streaming query: dropDuplicatesWithinWatermark has no batch mode
        rows = stream_collect(self, [txn("T1"), txn("T1"), txn("T1"), txn("T2")], dedup)
        self.assertEqual(sorted(r["txn_id"] for r in rows), ["T1", "T2"])

    def test_dedup_keeps_distinct_ids_across_wallets(self):
        rows = stream_collect(self, [txn("T1", wallet_id="W1"), txn("T2", wallet_id="W1"), txn("T3", wallet_id="W2")], dedup)
        self.assertEqual(len(rows), 3)

    def test_enrichment_columns(self):
        r = self.run_silver([txn("T1", wallet_id="W0002", amount=250.0)])["T1"]
        self.assertEqual((r["segment"], r["city"]), ("new", "Izmir"))
        self.assertAlmostEqual(r["amount_to_limit"], 0.25)
        self.assertEqual(str(r["txn_date"]), "2026-01-01")

    def test_high_value_threshold_is_inclusive(self):
        out = self.run_silver([txn("T1", amount=1999.99), txn("T2", amount=2000.0)])
        self.assertFalse(out["T1"]["is_high_value"])
        self.assertTrue(out["T2"]["is_high_value"])

    def test_unknown_wallet_is_kept_with_null_segment(self):
        # left join: no data loss even if the dimension table is incomplete (verify.py flags this separately)
        r = self.run_silver([txn("T1", wallet_id="W9999")])["T1"]
        self.assertIsNone(r["segment"])
