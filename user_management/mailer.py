from __future__ import annotations

import json
import os
import smtplib
from dataclasses import dataclass
from email.message import EmailMessage
from typing import Optional


@dataclass(frozen=True)
class SmtpConfig:
    host: str
    port: int
    use_starttls: bool
    username: Optional[str]
    password: Optional[str]
    from_addr: str
    reply_to: Optional[str]


def load_smtp_config(path: str) -> SmtpConfig:
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    password = None
    pw_env = raw.get("password_env")
    if pw_env:
        password = os.environ.get(pw_env)

    return SmtpConfig(
        host=raw["host"],
        port=int(raw.get("port", 587)),
        use_starttls=bool(raw.get("use_starttls", True)),
        username=raw.get("username"),
        password=password,
        from_addr=raw["from_addr"],
        reply_to=raw.get("reply_to"),
    )


def build_warning_email(
    *,
    to_addr: str,
    username: str,
    full_name: str | None,
    days_inactive: int,
    days_until_demotion: int,
    portal_home_url: str,
    from_addr: str,
    reply_to: str | None,
) -> EmailMessage:
    display = full_name or username

    subject = f"ArcGIS Enterprise inactivity notice ({days_until_demotion} day(s) until viewer-only)"
    body = f"""Hello {display},

Our records show you have not logged into ArcGIS Enterprise for {days_inactive} day(s).

Per policy, accounts inactive for 35 days are automatically reduced to “Viewer” (viewer-only) access.
If you still require non-viewer access, please log in before the demotion date.

Portal URL:
  {portal_home_url}

If you believe this notice is in error, reply to this email.

Thank you,
GIS Administration
"""

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = to_addr
    if reply_to:
        msg["Reply-To"] = reply_to
    msg.set_content(body)
    return msg


def build_demoted_email(
    *,
    to_addr: str,
    username: str,
    full_name: str | None,
    days_inactive: int,
    portal_home_url: str,
    from_addr: str,
    reply_to: str | None,
) -> EmailMessage:
    display = full_name or username

    subject = "ArcGIS Enterprise access change: account reduced to Viewer"
    body = f"""Hello {display},

Because your account has been inactive for {days_inactive} day(s), it has been automatically reduced to “Viewer” (viewer-only) access.

Portal URL:
  {portal_home_url}

If you need your previous access restored, please log in and then contact GIS Administration (or reply to this email).

Thank you,
GIS Administration
"""

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = to_addr
    if reply_to:
        msg["Reply-To"] = reply_to
    msg.set_content(body)
    return msg


def send_email(cfg: SmtpConfig, msg: EmailMessage) -> None:
    with smtplib.SMTP(cfg.host, cfg.port, timeout=30) as s:
        if cfg.use_starttls:
            s.ehlo()
            s.starttls()
            s.ehlo()
        if cfg.username and cfg.password:
            s.login(cfg.username, cfg.password)
        s.send_message(msg)