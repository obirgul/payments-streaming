"""Producer <-> rest of the project consistency (see 'Birbirine bagli ayarlar' in CLAUDE.md)."""
import argparse
import os
import random
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

import producer
from common import REF, TXN_SCHEMA
from spark_base import SparkTestCase
from transforms import velocity_alerts


class WalletsCsvTest(unittest.TestCase):
    def test_wallets_csv_matches_generator(self):
        with tempfile.TemporaryDirectory() as d:
            out = os.path.join(d, "wallets.csv")
            producer.gen_wallets(argparse.Namespace(out=out))
            with open(out) as f:
                generated = f.read()
        with open(os.path.join(REF, "wallets.csv")) as f:
            committed = f.read()
        self.assertEqual(generated, committed, "wallets.csv is stale: re-run `producer.py gen-wallets`")

    def test_wallet_count_matches_n_wallets(self):
        with open(os.path.join(REF, "wallets.csv")) as f:
            self.assertEqual(len(f.read().strip().splitlines()) - 1, producer.N_WALLETS)


class EventShapeTest(unittest.TestCase):
    def test_event_matches_txn_schema(self):
        ev = producer.new_event(random.Random(1), 1, datetime.now(timezone.utc))
        self.assertEqual(set(ev), {f.name for f in TXN_SCHEMA.fields})

    def test_event_time_is_iso8601_utc_millis(self):
        ev = producer.new_event(random.Random(1), 1, datetime(2026, 1, 1, 10, 0, 0, 123456, tzinfo=timezone.utc))
        self.assertEqual(ev["event_time"], "2026-01-01T10:00:00.123Z")


class NormalTrafficTest(SparkTestCase):
    def test_normal_traffic_does_not_trigger_fraud_alerts(self):
        """Producer's normal traffic (200 wallets, 5 ev/s) must stay under the velocity threshold.
        Deterministic: fixed seed, synthetic timestamps."""
        rng = random.Random(producer_seed())
        start = datetime(2026, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        events = []
        for i in range(5 * 240):  # 240 s at 5 ev/s
            events.append(producer.new_event(rng, rng.randint(1, producer.N_WALLETS), start + timedelta(seconds=i / 5.0)))
        alerts = velocity_alerts(self.txns(events)).collect()
        self.assertEqual([(r["wallet_id"], r["txn_count"]) for r in alerts], [])


def producer_seed():
    return 7  # producer's default --seed
