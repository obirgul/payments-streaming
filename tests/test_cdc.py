import shutil
import tempfile

from pyspark.sql import functions as F

from spark_base import SparkTestCase
from transforms import apply_cdc_batch, latest_per_wallet, parse_cdc


def row(wid, bal, status="ACTIVE"):
    return {"wallet_id": wid, "user_id": "U" + wid, "balance_kurus": bal, "status": status}


def event(op, lsn, before=None, after=None):
    return {"op": op, "ts_ms": 1000 + lsn, "source": {"db": "payments", "table": "wallets", "lsn": lsn},
            "before": before, "after": after}


class CdcParseTest(SparkTestCase):
    def test_create_update_delete(self):
        rows = parse_cdc(self.json_df([
            event("c", 1, after=row("W1", 100)),
            event("u", 2, before=row("W1", 100), after=row("W1", 150)),
            event("d", 3, before=row("W1", 150)),
        ])).orderBy("lsn").collect()
        self.assertEqual([r["op"] for r in rows], ["c", "u", "d"])
        self.assertEqual([r["balance_kurus"] for r in rows], [100, 150, None])
        # on delete the key comes from `before`
        self.assertEqual({r["wallet_id"] for r in rows}, {"W1"})

    def test_latest_per_wallet_keeps_highest_lsn(self):
        df = parse_cdc(self.json_df([
            event("u", 5, after=row("W1", 500)),
            event("u", 9, after=row("W1", 900)),
            event("u", 7, after=row("W1", 700)),
            event("u", 2, after=row("W2", 200)),
        ]))
        got = {r["wallet_id"]: r["balance_kurus"] for r in latest_per_wallet(df).collect()}
        self.assertEqual(got, {"W1": 900, "W2": 200})


class CdcMergeTest(SparkTestCase):
    def setUp(self):
        self.path = tempfile.mkdtemp(prefix="wallets_delta_")
        self.addCleanup(shutil.rmtree, self.path, True)

    def apply(self, *events):
        apply_cdc_batch(self.spark, parse_cdc(self.json_df(list(events))), self.path)

    def state(self):
        return {r["wallet_id"]: r["balance_kurus"] for r in self.spark.read.format("delta").load(self.path).collect()}

    def test_insert_and_update(self):
        self.apply(event("c", 1, after=row("W1", 100)), event("c", 2, after=row("W2", 200)))
        self.apply(event("u", 3, before=row("W1", 100), after=row("W1", 150)))
        self.assertEqual(self.state(), {"W1": 150, "W2": 200})

    def test_many_updates_in_one_batch_apply_the_latest(self):
        self.apply(event("c", 1, after=row("W1", 100)),
                   event("u", 2, after=row("W1", 120)),
                   event("u", 3, after=row("W1", 130)))
        self.assertEqual(self.state(), {"W1": 130})

    def test_delete(self):
        self.apply(event("c", 1, after=row("W1", 100)), event("c", 2, after=row("W2", 200)))
        self.apply(event("d", 3, before=row("W1", 100)))
        self.assertEqual(self.state(), {"W2": 200})

    def test_replayed_batch_is_idempotent(self):
        batch = [event("c", 1, after=row("W1", 100)), event("u", 2, after=row("W1", 150))]
        self.apply(*batch)
        self.apply(*batch)
        self.assertEqual(self.state(), {"W1": 150})

    def test_stale_update_does_not_overwrite_newer_state(self):
        self.apply(event("c", 1, after=row("W1", 100)), event("u", 5, after=row("W1", 500)))
        self.apply(event("u", 3, after=row("W1", 300)))
        self.assertEqual(self.state(), {"W1": 500})

    def test_stale_delete_does_not_remove_newer_state(self):
        self.apply(event("c", 1, after=row("W1", 100)), event("u", 5, after=row("W1", 500)))
        self.apply(event("d", 3, before=row("W1", 100)))
        self.assertEqual(self.state(), {"W1": 500})

    def test_delete_of_unknown_wallet_is_ignored(self):
        self.apply(event("c", 1, after=row("W1", 100)))
        self.apply(event("d", 2, before=row("W9", 1)))
        self.assertEqual(self.state(), {"W1": 100})

    def test_lsn_is_stored(self):
        self.apply(event("c", 1, after=row("W1", 100)), event("u", 4, after=row("W1", 150)))
        lsn = self.spark.read.format("delta").load(self.path).select(F.max("lsn")).first()[0]
        self.assertEqual(lsn, 4)
