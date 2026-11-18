import unittest

from alert_compare import compare_alerts


class CompareAlertsTest(unittest.TestCase):
    def test_identical(self):
        a = [("W1", "t1", 6), ("W2", "t2", 7)]
        d = compare_alerts(a, list(a))
        self.assertEqual((d["only_spark"], d["only_flink"], d["count_mismatch"], d["common"]), ([], [], [], 2))

    def test_spark_update_mode_repeats_collapse_to_max(self):
        spark = [("W1", "t1", 6), ("W1", "t1", 8), ("W1", "t1", 7)]
        d = compare_alerts(spark, [("W1", "t1", 8)])
        self.assertEqual(d["count_mismatch"], [])

    def test_only_one_side(self):
        d = compare_alerts([("W1", "t1", 6)], [("W2", "t1", 6)])
        self.assertEqual(d["only_spark"], [("W1", "t1")])
        self.assertEqual(d["only_flink"], [("W2", "t1")])

    def test_count_mismatch(self):
        d = compare_alerts([("W1", "t1", 6)], [("W1", "t1", 7)])
        self.assertEqual(d["count_mismatch"], [(("W1", "t1"), 6, 7)])

    def test_direction_of_count_difference(self):
        d = compare_alerts([("W1", "t1", 12), ("W2", "t1", 6)], [("W1", "t1", 11), ("W2", "t1", 7)])
        self.assertEqual(d["spark_higher"], [(("W1", "t1"), 12, 11)])
        self.assertEqual(d["flink_higher"], [(("W2", "t1"), 6, 7)])
