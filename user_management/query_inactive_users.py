#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


TABLES = {"tracked_users", "demotion_archive"}

# For range queries: which timestamp column each table uses
DATE_COL = {
    "tracked_users": "last_seen_utc",
    "demotion_archive": "demoted_utc",
}


def connect_ro(db_path: Path) -> sqlite3.Connection:
    # Read-only connection (URI mode)
    uri = f"file:{db_path.as_posix()}?mode=ro"
    return sqlite3.connect(uri, uri=True)


def parse_day(s: str) -> dt.date:
    try:
        return dt.date.fromisoformat(s)
    except ValueError:
        raise SystemExit(f"Invalid date '{s}'. Use YYYY-MM-DD.")


def day_bounds_utc(start_day: dt.date, stop_day: dt.date) -> Tuple[str, str]:
    """
    Inclusive full-day range:
      [start_day 00:00:00Z, stop_day+1 00:00:00Z)
    Stored timestamps are ISO strings with timezone (we write ...+00:00).
    Lexicographic comparison works for ISO-8601 timestamps with the same offset format.
    """
    start_dt = dt.datetime.combine(start_day, dt.time(0, 0, 0), tzinfo=dt.timezone.utc)
    stop_dt_exclusive = dt.datetime.combine(stop_day + dt.timedelta(days=1), dt.time(0, 0, 0), tzinfo=dt.timezone.utc)
    return start_dt.isoformat(), stop_dt_exclusive.isoformat()


def fetch_all(conn: sqlite3.Connection, table: str) -> List[Dict[str, Any]]:
    cur = conn.execute(f"SELECT * FROM {table} ORDER BY rowid ASC")
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def fetch_by_username(conn: sqlite3.Connection, table: str, username: str) -> List[Dict[str, Any]]:
    if table == "tracked_users":
        cur = conn.execute("SELECT * FROM tracked_users WHERE username = ? ORDER BY username", (username,))
    else:
        cur = conn.execute(
            "SELECT * FROM demotion_archive WHERE username = ? ORDER BY id DESC",
            (username,),
        )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def fetch_by_email(conn: sqlite3.Connection, table: str, email: str) -> List[Dict[str, Any]]:
    if table == "tracked_users":
        cur = conn.execute("SELECT * FROM tracked_users WHERE email = ? ORDER BY username", (email,))
    else:
        cur = conn.execute(
            "SELECT * FROM demotion_archive WHERE email = ? ORDER BY id DESC",
            (email,),
        )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def fetch_by_date_range(conn: sqlite3.Connection, table: str, start: dt.date, stop: dt.date) -> List[Dict[str, Any]]:
    col = DATE_COL[table]
    start_iso, stop_iso_excl = day_bounds_utc(start, stop)

    # Use >= start and < stop_exclusive (full days, inclusive stop day)
    cur = conn.execute(
        f"SELECT * FROM {table} WHERE {col} >= ? AND {col} < ? ORDER BY {col} ASC",
        (start_iso, stop_iso_excl),
    )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def print_json(rows: Sequence[Dict[str, Any]]) -> None:
    print(json.dumps(list(rows), indent=2, sort_keys=True, default=str))


def print_csv(rows: Sequence[Dict[str, Any]]) -> None:
    if not rows:
        return
    fieldnames = sorted(rows[0].keys())
    w = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
    w.writeheader()
    for r in rows:
        w.writerow(r)


def ensure_table_exists(conn: sqlite3.Connection, table: str) -> None:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    if not row:
        raise SystemExit(f"Table '{table}' not found in database.")


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Read-only admin utility for inactive_users.db (tracked_users + demotion_archive)."
    )
    ap.add_argument("--db", default="inactive_users.db", help="Path to SQLite DB (default: inactive_users.db)")
    ap.add_argument("--table", default="demotion_archive", choices=sorted(TABLES),
                    help="Which table to query (default: demotion_archive)")
    ap.add_argument("--csv", action="store_true", help="Output CSV instead of JSON")

    sub = ap.add_subparsers(dest="cmd", required=True)

    sub.add_parser("dump", help="Dump all rows from the selected table")

    p_user = sub.add_parser("user", help="Query by username")
    p_user.add_argument("--username", required=True)

    p_email = sub.add_parser("email", help="Query by email")
    p_email.add_argument("--email", required=True)

    p_range = sub.add_parser("range", help="Query by date range (full days, inclusive)")
    p_range.add_argument("--start", required=True, help="YYYY-MM-DD")
    p_range.add_argument("--stop", required=True, help="YYYY-MM-DD")

    args = ap.parse_args()
    db_path = Path(args.db)

    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path}")

    conn = connect_ro(db_path)
    try:
        ensure_table_exists(conn, args.table)

        if args.cmd == "dump":
            rows = fetch_all(conn, args.table)
        elif args.cmd == "user":
            rows = fetch_by_username(conn, args.table, args.username)
        elif args.cmd == "email":
            rows = fetch_by_email(conn, args.table, args.email)
        elif args.cmd == "range":
            start = parse_day(args.start)
            stop = parse_day(args.stop)
            if stop < start:
                raise SystemExit("--stop must be >= --start")
            rows = fetch_by_date_range(conn, args.table, start, stop)
        else:
            raise SystemExit("Unknown command")

    finally:
        conn.close()

    if args.csv:
        print_csv(rows)
    else:
        print_json(rows)

    # Exit code 0 even if no rows; that's normal for lookups.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())