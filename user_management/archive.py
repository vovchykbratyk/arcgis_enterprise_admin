from __future__ import annotations

import datetime as dt
import sqlite3
from dataclasses import dataclass
from typing import Optional


ARCHIVE_SCHEMA = """
CREATE TABLE IF NOT EXISTS demotion_archive (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  username TEXT NOT NULL,
  email TEXT,
  full_name TEXT,
  demoted_utc TEXT NOT NULL,

  previous_role TEXT,
  previous_user_type TEXT,

  new_role TEXT NOT NULL,
  new_user_type TEXT NOT NULL,

  inactive_days INTEGER
);

CREATE INDEX IF NOT EXISTS idx_demotion_archive_username ON demotion_archive(username);
CREATE INDEX IF NOT EXISTS idx_demotion_archive_demoted_utc ON demotion_archive(demoted_utc);
"""


@dataclass(frozen=True)
class DemotionRecord:
    username: str
    email: Optional[str]
    full_name: Optional[str]
    demoted_utc: dt.datetime
    previous_role: Optional[str]
    previous_user_type: Optional[str]
    new_role: str
    new_user_type: str
    inactive_days: int


def ensure_archive_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(ARCHIVE_SCHEMA)


def archive_demotion(conn: sqlite3.Connection, rec: DemotionRecord) -> None:
    conn.execute(
        """
        INSERT INTO demotion_archive(
          username, email, full_name, demoted_utc,
          previous_role, previous_user_type,
          new_role, new_user_type,
          inactive_days
        )
        VALUES(?,?,?,?,?,?,?,?,?)
        """,
        (
            rec.username,
            rec.email,
            rec.full_name,
            rec.demoted_utc.isoformat(),
            rec.previous_role,
            rec.previous_user_type,
            rec.new_role,
            rec.new_user_type,
            rec.inactive_days,
        ),
    )


def latest_demotion_for_user(conn: sqlite3.Connection, username: str) -> Optional[dict]:
    row = conn.execute(
        """
        SELECT username, email, full_name, demoted_utc,
               previous_role, previous_user_type,
               new_role, new_user_type,
               inactive_days
        FROM demotion_archive
        WHERE username = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (username,),
    ).fetchone()

    if not row:
        return None

    keys = [
        "username", "email", "full_name", "demoted_utc",
        "previous_role", "previous_user_type",
        "new_role", "new_user_type",
        "inactive_days",
    ]
    return dict(zip(keys, row))