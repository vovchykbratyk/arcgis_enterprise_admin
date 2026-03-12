import datetime as dt
import unittest

from portal_audit import days_inactive_from_last_login
from user_latency_guard import inactivity_bucket


class TestPolicyMath(unittest.TestCase):
    def test_days_inactive_flooring(self):
        now = dt.datetime(2026, 3, 5, 12, 0, 0, tzinfo=dt.timezone.utc)

        # exactly now -> 0 days
        last_ms = int(now.timestamp() * 1000)
        self.assertEqual(days_inactive_from_last_login(last_ms, now), 0)

        # 1 day minus 1 second -> still 0
        last = now - dt.timedelta(days=1) + dt.timedelta(seconds=1)
        self.assertEqual(days_inactive_from_last_login(int(last.timestamp() * 1000), now), 0)

        # exactly 1 day -> 1
        last = now - dt.timedelta(days=1)
        self.assertEqual(days_inactive_from_last_login(int(last.timestamp() * 1000), now), 1)

        # missing -> None
        self.assertIsNone(days_inactive_from_last_login(None, now))

    def test_bucketization(self):
        self.assertEqual(inactivity_bucket(0), "0-14")
        self.assertEqual(inactivity_bucket(14), "0-14")
        self.assertEqual(inactivity_bucket(15), "15-19")
        self.assertEqual(inactivity_bucket(19), "15-19")
        self.assertEqual(inactivity_bucket(20), "20-27")
        self.assertEqual(inactivity_bucket(27), "20-27")
        self.assertEqual(inactivity_bucket(28), "28-34")
        self.assertEqual(inactivity_bucket(34), "28-34")
        self.assertEqual(inactivity_bucket(35), "35+")
        self.assertEqual(inactivity_bucket(120), "35+")