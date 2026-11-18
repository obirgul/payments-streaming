from datetime import datetime

from spark_base import SparkTestCase, txn


class ParseTransactionsTest(SparkTestCase):
    def test_valid_record_is_typed(self):
        rows = self.txns([txn("T1", amount=12.5)]).collect()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["amount"], 12.5)
        self.assertEqual(rows[0]["event_time"], datetime(2026, 1, 1, 10, 0, 0))

    def test_malformed_json_is_dropped(self):
        rows = self.txns([txn("T1"), "{not json", ""]).collect()
        self.assertEqual([r["txn_id"] for r in rows], ["T1"])

    def test_missing_keys_are_dropped(self):
        no_id = txn("T1")
        del no_id["txn_id"]
        no_wallet = txn("T2")
        no_wallet["wallet_id"] = None
        rows = self.txns([no_id, no_wallet, txn("T3")]).collect()
        self.assertEqual([r["txn_id"] for r in rows], ["T3"])

    def test_unparseable_event_time_becomes_null(self):
        rows = self.txns([txn("T1", event_time="yesterday")]).collect()
        self.assertIsNone(rows[0]["event_time"])
