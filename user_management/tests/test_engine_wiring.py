import datetime as dt
import tempfile
import unittest
from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple

from portal_audit import PortalUser
from user_latency_guard import Policy, RunMode, init_db, run_policy


@dataclass
class FakeSmtpCfg:
    from_addr: str = "gis-admin@example.com"
    reply_to: str | None = "helpdesk@example.com"


class FakeMailer:
    def __init__(self) -> None:
        self.sent: List[object] = []

    def send(self, _cfg, msg) -> None:
        self.sent.append(msg)


class FakePortalMutator:
    def __init__(self) -> None:
        self.role_calls: List[Tuple[str, str]] = []
        self.ult_calls: List[Tuple[str, str]] = []

    def set_role(self, username: str, role: str) -> None:
        self.role_calls.append((username, role))

    def set_ult(self, username: str, ult: str) -> None:
        self.ult_calls.append((username, ult))


def ms_for_days_inactive(now: dt.datetime, days: int) -> int:
    # exactly N days inactive
    last = now - dt.timedelta(days=days)
    return int(last.timestamp() * 1000)


class TestEngineWiring(unittest.TestCase):
    def test_wiring_warn_demote_skip_archive(self):
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / "inactive_users.db"
            conn = init_db(db)
            try:
                now = dt.datetime(2026, 3, 5, 12, 0, 0, tzinfo=dt.timezone.utc)

                policy = Policy(
                    connection="unused",
                    portal_home_url="https://portal/home/",
                    demote_after_days=35,
                    warning_days={15, 20, 28, 29, 30, 31, 32, 33, 34},
                    treat_never_logged_in_as_days_inactive=35,
                    viewer_role="viewer",
                    viewer_user_license_type_id="viewerUT",
                    skip_usernames={"admin", "svc"},
                    skip_roles={"org_admin"},
                    sqlite_db_path=str(db),
                    smtp_json_path="unused",
                )

                users = [
                    # skipped by username
                    PortalUser("admin", "a@x", "Admin", "publisher", "creatorUT", ms_for_days_inactive(now, 40)),
                    # skipped by role
                    PortalUser("role_skip", "rs@x", "Role Skip", "org_admin", "creatorUT", ms_for_days_inactive(now, 40)),
                    # missing email, should demote at 35 (but no email)
                    PortalUser("noemail", None, "No Email", "publisher", "creatorUT", ms_for_days_inactive(now, 35)),
                    # warn at 15
                    PortalUser("warn15", "w15@x", "Warn 15", "publisher", "creatorUT", ms_for_days_inactive(now, 15)),
                    # warn at 34
                    PortalUser("warn34", "w34@x", "Warn 34", "publisher", "creatorUT", ms_for_days_inactive(now, 34)),
                    # demote at 35
                    PortalUser("demote35", "d35@x", "Demote 35", "publisher", "creatorUT", ms_for_days_inactive(now, 35)),
                    # already viewer (no action)
                    PortalUser("already", "al@x", "Already", "viewer", "viewerUT", ms_for_days_inactive(now, 100)),
                ]

                mailer = FakeMailer()
                mut = FakePortalMutator()
                smtp = FakeSmtpCfg()

                res = run_policy(
                    conn=conn,
                    users=users,
                    policy=policy,
                    now_utc=now,
                    mode=RunMode.LIVE,
                    smtp_cfg=smtp,
                    send_email_fn=mailer.send,
                    set_role_fn=mut.set_role,
                    set_ult_fn=mut.set_ult,
                    emit=lambda s: None,
                )

                # scanned counts include users with usernames (all provided)
                self.assertEqual(res.scanned, 7)
                # skipped: admin + role_skip
                self.assertEqual(res.skipped, 2)

                # warned: warn15 + warn34 (2)
                self.assertEqual(res.warned, 2)

                # demoted: noemail + demote35 (2)
                self.assertEqual(res.demoted, 2)

                # already viewer: 1
                self.assertEqual(res.already_viewer, 1)

                # missing email: 1 (noemail)
                self.assertEqual(res.missing_email, 1)

                # Mail sent:
                # - 2 warnings
                # - demoted email for demote35 only (noemail has no address)
                self.assertEqual(len(mailer.sent), 3)

                # Mutations called for two demotions: role + ult each
                self.assertEqual(mut.role_calls, [("noemail", "viewer"), ("demote35", "viewer")])
                self.assertEqual(mut.ult_calls, [("noemail", "viewerUT"), ("demote35", "viewerUT")])

                # Archive contains demote35 + noemail with previous_user_type == creatorUT
                rows = conn.execute(
                    "SELECT username, previous_user_type, new_user_type FROM demotion_archive ORDER BY id ASC"
                ).fetchall()
                self.assertEqual(len(rows), 2)
                self.assertEqual(rows[0][1], "creatorUT")
                self.assertEqual(rows[0][2], "viewerUT")
                self.assertEqual(rows[1][1], "creatorUT")
                self.assertEqual(rows[1][2], "viewerUT")

            finally:
                conn.close()

    def test_report_mode_populates_would_lists(self):
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / "inactive_users.db"
            conn = init_db(db)
            try:
                now = dt.datetime(2026, 3, 5, 12, 0, 0, tzinfo=dt.timezone.utc)

                policy = Policy(
                    connection="unused",
                    portal_home_url="https://portal/home/",
                    demote_after_days=35,
                    warning_days={15},
                    treat_never_logged_in_as_days_inactive=35,
                    viewer_role="viewer",
                    viewer_user_license_type_id="viewerUT",
                    skip_usernames=set(),
                    skip_roles=set(),
                    sqlite_db_path=str(db),
                    smtp_json_path="unused",
                )

                users = [
                    PortalUser("warn15", "w15@x", "Warn 15", "publisher", "creatorUT", ms_for_days_inactive(now, 15)),
                    PortalUser("demote35", "d35@x", "Demote 35", "publisher", "creatorUT", ms_for_days_inactive(now, 35)),
                ]

                res = run_policy(
                    conn=conn,
                    users=users,
                    policy=policy,
                    now_utc=now,
                    mode=RunMode.REPORT,
                    smtp_cfg=FakeSmtpCfg(),
                    send_email_fn=lambda *_: None,
                    set_role_fn=lambda *_: None,
                    set_ult_fn=lambda *_: None,
                    emit=lambda s: None,
                )

                self.assertEqual(len(res.would_warn), 1)
                self.assertEqual(res.would_warn[0][0], "warn15")
                self.assertEqual(len(res.would_demote), 1)
                self.assertEqual(res.would_demote[0][0], "demote35")

                # Report mode should not write archive rows
                cnt = conn.execute("SELECT COUNT(*) FROM demotion_archive").fetchone()[0]
                self.assertEqual(cnt, 0)

            finally:
                conn.close()