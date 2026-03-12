import datetime as dt
import sqlite3
import tempfile
import unittest
from pathlib import Path

from archive import DemotionRecord, archive_demotion
from user_latency_guard import (
    init_db,
    add_sent_warning_day,
    get_sent_warning_days,
    upsert_user_snapshot,
)
from portal_audit import PortalUser


class TestDbShapes(unittest.TestCase):
    def test_schema_and_round_trip(self):
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / "inactive_users.db"
            conn = init_db(db)

            try:
                # tables exist
                names = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
                self.assertIn("tracked_users", names)
                self.assertIn("demotion_archive", names)

                # upsert snapshot
                now = dt.datetime(2026, 3, 5, 12, 0, 0, tzinfo=dt.timezone.utc)
                u = PortalUser(
                    username="jdoe",
                    email="jdoe@example.com",
                    full_name="Jane Doe",
                    role="publisher",
                    user_type="creatorUT",
                    last_login_ms=123,
                )
                upsert_user_snapshot(conn, u, now, inactive_days=20)
                conn.commit()

                row = conn.execute("SELECT username, email, last_inactive_days FROM tracked_users WHERE username='jdoe'").fetchone()
                self.assertEqual(row[0], "jdoe")
                self.assertEqual(row[1], "jdoe@example.com")
                self.assertEqual(row[2], 20)

                # warning day tracking
                self.assertEqual(get_sent_warning_days(conn, "jdoe"), set())
                add_sent_warning_day(conn, "jdoe", 15)
                conn.commit()
                self.assertEqual(get_sent_warning_days(conn, "jdoe"), {15})
                add_sent_warning_day(conn, "jdoe", 20)
                conn.commit()
                self.assertEqual(get_sent_warning_days(conn, "jdoe"), {15, 20})

                # archive insertion
                rec = DemotionRecord(
                    username="jdoe",
                    email="jdoe@example.com",
                    full_name="Jane Doe",
                    demoted_utc=now,
                    previous_role="publisher",
                    previous_user_type="creatorUT",
                    new_role="viewer",
                    new_user_type="viewerUT",
                    inactive_days=35,
                )
                archive_demotion(conn, rec)
                conn.commit()

                arow = conn.execute(
                    "SELECT username, previous_user_type, new_user_type FROM demotion_archive WHERE username='jdoe' ORDER BY id DESC LIMIT 1"
                ).fetchone()
                self.assertEqual(arow[0], "jdoe")
                self.assertEqual(arow[1], "creatorUT")
                self.assertEqual(arow[2], "viewerUT")

            finally:
                conn.close()