from datetime import datetime, timedelta

from spark_base import SparkTestCase, txn
from transforms import DEFAULT_THRESHOLD, alerts_to_kafka, velocity_alerts

T0 = datetime(2026, 1, 1, 10, 0, 0)


def at(seconds):
    return (T0 + timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%S.000Z")


class VelocityTest(SparkTestCase):
    def alerts(self, events, **kw):
        return velocity_alerts(self.txns(events), **kw).collect()

    def burst(self, wallet, n, gap, start=0):
        return [txn("%s-%d-%d" % (wallet, start, i), wallet_id=wallet, event_time=at(start + i * gap)) for i in range(n)]

    def test_default_threshold_is_six(self):
        # Flink SQL uses the same value (flink/sql/fraud_velocity.sql); keep them in sync
        self.assertEqual(DEFAULT_THRESHOLD, 6)

    def test_six_in_window_alerts(self):
        out = self.alerts(self.burst("W1", 6, gap=4))  # 0..20 s
        self.assertTrue(out)
        self.assertEqual({r["wallet_id"] for r in out}, {"W1"})
        self.assertEqual(max(r["txn_count"] for r in out), 6)

    def test_five_in_window_does_not_alert(self):
        self.assertEqual(self.alerts(self.burst("W1", 5, gap=4)), [])

    def test_slow_trickle_does_not_alert(self):
        # 6 events but 10 s apart: no 30 s window holds more than 3
        self.assertEqual(self.alerts(self.burst("W1", 6, gap=10)), [])

    def test_wallets_are_counted_separately(self):
        events = self.burst("W1", 3, gap=2) + self.burst("W2", 3, gap=2)
        self.assertEqual(self.alerts(events), [])

    def test_only_the_bursting_wallet_alerts(self):
        events = self.burst("W1", 8, gap=2) + self.burst("W2", 2, gap=2)
        self.assertEqual({r["wallet_id"] for r in self.alerts(events)}, {"W1"})

    def test_total_amount_is_summed(self):
        events = [txn("T%d" % i, event_time=at(i), amount=10.5) for i in range(6)]
        self.assertEqual(max(r["total_amount"] for r in self.alerts(events)), 63.0)

    def test_custom_threshold(self):
        self.assertTrue(self.alerts(self.burst("W1", 3, gap=1), threshold=3))
        self.assertEqual(self.alerts(self.burst("W1", 3, gap=1), threshold=4), [])

    def test_kafka_payload(self):
        row = alerts_to_kafka(velocity_alerts(self.txns(self.burst("W1", 6, gap=1)))).first()
        self.assertEqual(row["key"], "W1")
        for field in ("wallet_id", "window_start", "window_end", "txn_count", "total_amount", '"engine":"spark"'):
            self.assertIn(field, row["value"])
