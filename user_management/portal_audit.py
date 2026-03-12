from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any, Dict, Iterator, Optional


@dataclass(frozen=True)
class PortalUser:
    username: str
    email: Optional[str]
    full_name: Optional[str]
    role: Optional[str]
    user_type: Optional[str]
    last_login_ms: Optional[int]


class PortalClient:
    """
    Uses an authenticated arcgis.gis.GIS instance for all REST calls.
    Token refresh and secure storage handled by age-oauth + ArcGIS API for Python.
    """

    def __init__(self, gis) -> None:
        self.gis = gis
        self.base = gis._con.baseurl.rstrip("/")

    def _get(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base}/{path.lstrip('/')}"
        p = dict(params)
        p["f"] = "json"
        js = self.gis._con.get(url, p)
        if isinstance(js, dict) and "error" in js:
            raise RuntimeError(f"Portal API error: {js['error']}")
        return js

    def _post(self, path: str, data: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base}/{path.lstrip('/')}"
        d = dict(data)
        d["f"] = "json"
        js = self.gis._con.post(url, d)
        if isinstance(js, dict) and "error" in js:
            raise RuntimeError(f"Portal API error: {js['error']}")
        return js

    def iter_users(self, page_size: int = 100) -> Iterator[PortalUser]:
        start = 1
        while True:
            js = self._get("portals/self/users", {"start": start, "num": page_size})
            users = js.get("users", []) or []
            for u in users:
                yield PortalUser(
                    username=u.get("username"),
                    email=u.get("email"),
                    full_name=u.get("fullName"),
                    role=u.get("role"),
                    user_type=u.get("userType"),
                    last_login_ms=u.get("lastLogin"),
                )
            next_start = js.get("nextStart", -1)
            if not next_start or next_start == -1:
                break
            start = int(next_start)

    def set_user_role(self, username: str, role: str) -> None:
        self._post("portals/self/updateUserRole", {"user": username, "role": role})

    def set_user_license_type(self, username: str, user_license_type_id: str) -> None:
        self._post(
            "portals/self/updateUserLicenseType",
            {"users": username, "userLicenseTypeId": user_license_type_id},
        )


def days_inactive_from_last_login(last_login_ms: Optional[int], now_utc: dt.datetime) -> Optional[int]:
    if not last_login_ms:
        return None
    last = dt.datetime.fromtimestamp(last_login_ms / 1000.0, tz=dt.timezone.utc)
    delta = now_utc - last
    return max(0, int(delta.total_seconds() // 86400))