from __future__ import annotations

import argparse
import datetime as dt
import json
import sqlite3
from collections import Counter
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Callable, Iterable, List, Optional, Sequence, Set, Tuple

from age_oauth import get_gis

from archive import DemotionRecord, ensure_archive_schema, archive_demotion
from mailer import (
    build_demoted_email,
    build_warning_email,
    load_smtp_config,
    send_email,
)
from portal_audit import PortalClient, PortalUser, days_inactive_from_last_login


SCHEMA = """
CREATE TABLE IF NOT EXISTS tracked_users (
  username TEXT PRIMARY KEY,
  email TEXT,
  full_name TEXT,
  last_login_ms INTEGER,
  last_seen_utc TEXT NOT NULL,
  last_role TEXT,
  last_user_type TEXT,
  last_inactive_days INTEGER,
  demoted_utc TEXT,
  warning_days_sent TEXT DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_tracked_users_inactive ON tracked_users(last_inactive_days);
"""


class RunMode(str, Enum):
    LIVE = "live"
    DRY_RUN = "dry_run"
    REPORT = "report"


@dataclass(frozen=True)
class Policy:
    connection: str
    portal_home_url: str

    demote_after_days: int
    warning_days: Set[int]
    treat_never_logged_in_as_days_inactive: int

    viewer_role: str
    viewer_user_license_type_id: str

    skip_usernames: Set[str]
    skip_roles: Set[str]

    sqlite_db_path: str
    smtp_json_path: str


@dataclass
class RunResults:
    timestamp_utc: dt.datetime
    scanned: int = 0
    skipped: int = 0
    warned: int = 0
    demoted: int = 0
    already_viewer: int = 0
    missing_email: int = 0
    dist: Counter = None  # type: ignore
    would_warn: List[Tuple[str, int]] = None  # type: ignore
    would_demote: List[Tuple[str, int]] = None  # type: ignore

    def __post_init__(self) -> None:
        if self.dist is None:
            self.dist = Counter()
        if self.would_warn is None:
            self.would_warn = []
        if self.would_demote is None:
            self.would_demote = []


def load_policy(path: str) -> Policy:
    with open(path, "r", encoding="utf-8") as f:
        p = json.load(f)

    inactivity = p["inactivity"]
    demotion = p["demotion"]
    exclusions = p.get("exclusions", {})
    storage = p.get("storage", {})
    email = p.get("email", {})

    return Policy(
        connection=p["connection"],
        portal_home_url=p["portal_home_url"],

        demote_after_days=int(inactivity.get("demote_after_days", 35)),
        warning_days=set(int(x) for x in inactivity.get("warning_days", [])),
        treat_never_logged_in_as_days_inactive=int(inactivity.get("treat_never_logged_in_as_days_inactive", 35)),

        viewer_role=demotion.get("viewer_role", "viewer"),
        viewer_user_license_type_id=demotion["viewer_user_license_type_id"],

        skip_usernames=set(exclusions.get("skip_usernames", [])),
        skip_roles=set(exclusions.get("skip_roles", [])),

        sqlite_db_path=storage.get("sqlite_db_path", "inactive_users.db"),
        smtp_json_path=email.get("smtp_json_path", "smtp.json"),
    )


def init_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.executescript(SCHEMA)
    ensure_archive_schema(conn)
    conn.commit()
    return conn


def get_sent_warning_days(conn: sqlite3.Connection, username: str) -> Set[int]:
    row = conn.execute(
        "SELECT warning_days_sent FROM tracked_users WHERE username = ?",
        (username,),
    ).fetchone()
    if not row or not row[0]:
        return set()
    out: Set[int] = set()
    for part in row[0].split(","):
        part = part.strip()
        if part.isdigit():
            out.add(int(part))
    return out


def add_sent_warning_day(conn: sqlite3.Connection, username: str, day: int) -> None:
    existing = get_sent_warning_days(conn, username)
    existing.add(day)
    serialized = ",".join(str(x) for x in sorted(existing))
    conn.execute(
        "UPDATE tracked_users SET warning_days_sent = ? WHERE username = ?",
        (serialized, username),
    )


def upsert_user_snapshot(
    conn: sqlite3.Connection,
    u: PortalUser,
    now_utc: dt.datetime,
    inactive_days: int,
) -> None:
    conn.execute(
        """
        INSERT INTO tracked_users(
          username, email, full_name, last_login_ms, last_seen_utc,
          last_role, last_user_type, last_inactive_days
        )
        VALUES(?,?,?,?,?,?,?,?)
        ON CONFLICT(username) DO UPDATE SET
          email=excluded.email,
          full_name=excluded.full_name,
          last_login_ms=excluded.last_login_ms,
          last_seen_utc=excluded.last_seen_utc,
          last_role=excluded.last_role,
          last_user_type=excluded.last_user_type,
          last_inactive_days=excluded.last_inactive_days
        """,
        (
            u.username,
            u.email,
            u.full_name,
            u.last_login_ms,
            now_utc.isoformat(),
            u.role,
            u.user_type,
            inactive_days,
        ),
    )


def mark_demoted(conn: sqlite3.Connection, username: str, now_utc: dt.datetime) -> None:
    conn.execute(
        "UPDATE tracked_users SET demoted_utc = ? WHERE username = ?",
        (now_utc.isoformat(), username),
    )


def inactivity_bucket(days: int) -> str:
    if days <= 14:
        return "0-14"
    if days <= 19:
        return "15-19"
    if days <= 27:
        return "20-27"
    if days <= 34:
        return "28-34"
    return "35+"


def render_distribution(counts: Counter) -> str:
    order = ["0-14", "15-19", "20-27", "28-34", "35+"]
    lines = ["Inactivity distribution", ""]
    for k in order:
        lines.append(f"{k:<8} {counts.get(k, 0)} user(s)")
    return "\n".join(lines)


# -------------------------
# Refactor: testable engine
# -------------------------

SendEmailFn = Callable[[object, object], None]  # (smtp_cfg, msg) -> None
SetRoleFn = Callable[[str, str], None]
SetUltFn = Callable[[str, str], None]
EmitFn = Callable[[str], None]


def run_policy(
    *,
    conn: sqlite3.Connection,
    users: Iterable[PortalUser],
    policy: Policy,
    now_utc: dt.datetime,
    mode: RunMode,
    smtp_cfg: object,
    send_email_fn: SendEmailFn,
    set_role_fn: SetRoleFn,
    set_ult_fn: SetUltFn,
    emit: Optional[EmitFn] = None,
) -> RunResults:
    """
    Core policy engine. No ArcGIS, no SMTP assumptions.
    All side effects are injected (send_email_fn, set_role_fn, set_ult_fn, emit).
    """
    if emit is None:
        emit = lambda s: None  # noqa: E731

    res = RunResults(timestamp_utc=now_utc)

    for u in users:
        if not u.username:
            continue
        res.scanned += 1

        if u.username in policy.skip_usernames:
            res.skipped += 1
            continue

        if u.role and u.role in policy.skip_roles:
            res.skipped += 1
            continue

        inactive_days_opt = days_inactive_from_last_login(u.last_login_ms, now_utc)
        inactive_days = (
            inactive_days_opt
            if inactive_days_opt is not None
            else policy.treat_never_logged_in_as_days_inactive
        )

        res.dist[inactivity_bucket(inactive_days)] += 1
        upsert_user_snapshot(conn, u, now_utc, inactive_days)

        if not u.email:
            res.missing_email += 1

        is_viewer_role = (u.role == policy.viewer_role)
        is_viewer_type = (u.user_type == policy.viewer_user_license_type_id)
        if is_viewer_role and is_viewer_type:
            res.already_viewer += 1

        sent_days = get_sent_warning_days(conn, u.username)

        # Warnings
        if u.email and (inactive_days in policy.warning_days) and (inactive_days not in sent_days):
            if mode == RunMode.REPORT:
                res.would_warn.append((u.username, inactive_days))
            else:
                days_until = max(0, policy.demote_after_days - inactive_days)
                msg = build_warning_email(
                    to_addr=u.email,
                    username=u.username,
                    full_name=u.full_name,
                    days_inactive=inactive_days,
                    days_until_demotion=days_until,
                    portal_home_url=policy.portal_home_url,
                    from_addr=getattr(smtp_cfg, "from_addr", ""),
                    reply_to=getattr(smtp_cfg, "reply_to", None),
                )
                if mode == RunMode.DRY_RUN:
                    emit(f"[DRY RUN] warn: user={u.username} inactive_days={inactive_days} to={u.email}")
                else:
                    send_email_fn(smtp_cfg, msg)
                    add_sent_warning_day(conn, u.username, inactive_days)
                res.warned += 1

        # Demotion
        if inactive_days >= policy.demote_after_days:
            if not (is_viewer_role and is_viewer_type):
                if mode == RunMode.REPORT:
                    res.would_demote.append((u.username, inactive_days))
                elif mode == RunMode.DRY_RUN:
                    emit(
                        f"[DRY RUN] demote: user={u.username} inactive_days={inactive_days} "
                        f"role->{policy.viewer_role} userLicenseTypeId->{policy.viewer_user_license_type_id}"
                    )
                    res.demoted += 1
                else:
                    # Capture pre-change state now; write archive only after successful changes.
                    rec = DemotionRecord(
                        username=u.username,
                        email=u.email,
                        full_name=u.full_name,
                        demoted_utc=now_utc,
                        previous_role=u.role,
                        previous_user_type=u.user_type,
                        new_role=policy.viewer_role,
                        new_user_type=policy.viewer_user_license_type_id,
                        inactive_days=inactive_days,
                    )

                    # Apply changes
                    set_role_fn(u.username, policy.viewer_role)
                    set_ult_fn(u.username, policy.viewer_user_license_type_id)

                    # Archive + mark
                    archive_demotion(conn, rec)
                    mark_demoted(conn, u.username, now_utc)

                    # Notify
                    if u.email:
                        msg = build_demoted_email(
                            to_addr=u.email,
                            username=u.username,
                            full_name=u.full_name,
                            days_inactive=inactive_days,
                            portal_home_url=policy.portal_home_url,
                            from_addr=getattr(smtp_cfg, "from_addr", ""),
                            reply_to=getattr(smtp_cfg, "reply_to", None),
                        )
                        send_email_fn(smtp_cfg, msg)

                    res.demoted += 1

        conn.commit()

    return res


# -------------------------
# CLI / wiring (production)
# -------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="ArcGIS Enterprise inactivity guard (warn + viewer-demote)")
    ap.add_argument("--policy", required=True, help="Path to policy.json")
    ap.add_argument("--dry-run", action="store_true", help="No changes, no emails; prints actions.")
    ap.add_argument("--report", action="store_true", help="Report-only: no changes, no emails (still refreshes snapshots).")
    args = ap.parse_args()

    if args.dry_run and args.report:
        raise SystemExit("Use only one of --dry-run or --report (or neither).")

    policy = load_policy(args.policy)

    mode = RunMode.LIVE
    if args.dry_run:
        mode = RunMode.DRY_RUN
    elif args.report:
        mode = RunMode.REPORT

    gis = get_gis(connection=policy.connection)
    client = PortalClient(gis)
    smtp_cfg = load_smtp_config(policy.smtp_json_path)

    db_path = Path(policy.sqlite_db_path)
    conn = init_db(db_path)

    now_utc = dt.datetime.now(dt.timezone.utc)

    try:
        res = run_policy(
            conn=conn,
            users=client.iter_users(page_size=100),
            policy=policy,
            now_utc=now_utc,
            mode=mode,
            smtp_cfg=smtp_cfg,
            send_email_fn=send_email,
            set_role_fn=client.set_user_role,
            set_ult_fn=client.set_user_license_type,
            emit=print,
        )
    finally:
        conn.close()

    # Reports (always)
    ts = res.timestamp_utc.strftime("%Y-%m-%d %H:%M:%S UTC")
    print()
    print("ArcGIS Enterprise Inactivity Policy Run")
    print("--------------------------------------")
    print(f"Timestamp:            {ts}")
    print(f"Users scanned:         {res.scanned}")
    print(f"Excluded users:        {res.skipped}")
    print(f"Users warned today:    {res.warned}")
    print(f"Users demoted today:   {res.demoted}")
    print(f"Users already viewer:  {res.already_viewer}")
    print(f"Users missing email:   {res.missing_email}")
    print(f"SQLite DB:             {db_path}")
    print()
    print(render_distribution(res.dist))
    print()

    if mode == RunMode.REPORT:
        print("Planned actions (report-only)")
        print()
        print(f"Warnings that would be sent today: {len(res.would_warn)}")
        for uname, d in sorted(res.would_warn, key=lambda x: (-x[1], x[0]))[:50]:
            print(f"  warn:   {uname} ({d} days inactive)")
        if len(res.would_warn) > 50:
            print(f"  ... and {len(res.would_warn) - 50} more")
        print()
        print(f"Demotions that would occur today: {len(res.would_demote)}")
        for uname, d in sorted(res.would_demote, key=lambda x: (-x[1], x[0]))[:50]:
            print(f"  demote: {uname} ({d} days inactive)")
        if len(res.would_demote) > 50:
            print(f"  ... and {len(res.would_demote) - 50} more")
        print()

    if mode == RunMode.DRY_RUN:
        print("Mode: DRY RUN (no emails, no changes)")
    elif mode == RunMode.REPORT:
        print("Mode: REPORT ONLY (no emails, no changes)")
    else:
        print("Mode: LIVE (emails and changes enabled)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())