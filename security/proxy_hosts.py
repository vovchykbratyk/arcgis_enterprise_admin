#!/usr/bin/env python3
"""
proxy_hosts.py

Purpose: Crawl an ArcGIS Enteprise instance (target 11.3 API) and build a list
         of external hostnames referenced in services.  Add those to an array
         for inclusion in allowedProxyHosts

Dependencies: age-oauth, arcgis

How to run (assuming Powershell): python .\proxy_hosts.py

HIGHLY ADVISED to run this as a sysadmin, as the user should be able to see
any and all items across the ArcGIS Enterprise instance.

Outputs:
    ./proxy-host-audit/allowed_proxy_hosts.json
    ./proxy-host-audit/proxy_host_evidence.csv
"""

from __future__ import annotations

import csv
import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import urlparse

from age_oauth import get_gis
from arcgis.gis import GIS, Item


# globals
CONNECTION = "myportal"
OUTPUT_DIRECTORY = Path("proxy-host-audit")

# Hostnames or DNS patterns to ignore
INTERNAL_HOSTS = {
    "agdf.army.ic.gov",
    ".army.ic.gov",
}

# data object
@dataclass(frozen=True)
class Finding:
    hostname: str
    url: str
    item_id: str
    item_title: str
    item_type: str
    source: str
    json_path: str


# regex for URL
URL_PATTERN = re.compile(
    r"""https?://[^\s"'<>\\)\]}]+""",
    flags=re.IGNORECASE,
)


def extract_urls(text: str) -> set[str]:
    """
    Extract HTTP and HTTPS URLs from text (HTML, popups, JSON, wherever)
    """
    urls: set[str] = set()

    for match in URL_PATTERN.findall(text):
        candidate = match.rstrip(".,;:!?)]}'\"")

        try:
            parsed = urlparse(candidate)
        except ValueError:
            continue

        if parsed.scheme.lower() not in {"http", "https"}:
            continue

        if not parsed.hostname:
            continue

        urls.add(candidate)

    return urls


def hostname_from_url(url: str) -> str | None:
    """
    Return a normalized destination hostname
    """
    try:
        hostname = urlparse(url).hostname
    except ValueError:
        return None

    if not hostname:
        return None

    return hostname.lower().rstrip(".")


def host_is_internal(hostname: str, internal_hosts: set[str]) -> bool:
    """
    Check if hostname is internal and thus ignored
    """
    hostname = hostname.lower().rstrip(".")

    for rule in internal_hosts:
        rule = rule.lower().rstrip(".")

        if rule.startswith("."):
            domain = rule[1:]

            if hostname == domain or hostname.endswith(f".{domain}"):
                return True

        elif hostname == rule:
            return True

    return False


def walk_json(value: Any, path: str = "$") -> Iterator[tuple[str, str]]:
    """
    generator producing each URL and JSON path
    """
    if isinstance(value, dict):
        for key, child in value.items():
            yield from walk_json(child, f"{path}.{key}")

    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from walk_json(child, f"{path}[{index}]")

    elif isinstance(value, str):
        for url in extract_urls(value):
            yield path, url


# inspection jobs
def all_organization_items(gis: GIS) -> list[Item]:
    """
    Returns all items visible to the authenticated user - this
    is why we run it as SA
    """
    organization_id = gis.properties.id

    return gis.content.search(
        query=f"orgid:{organization_id}",
        max_items=-1,
        outside_org=False,
    )


def item_properties(item: Item) -> dict[str, Any]:
    """
    Convert item metadata to dict
    """
    try:
        return dict(item)
    except (TypeError, ValueError):
        return {
            "id": item.id,
            "title": item.title,
            "type": item.type,
            "url": getattr(item, "url", None),
            "description": getattr(item, "description", None),
            "snippet": getattr(item, "snippet", None),
            "typeKeywords": getattr(item, "typeKeywords", None),
        }


def inspect_document(
    document: Any,
    *,
    item: Item,
    source: str,
    internal_hosts: set[str],
    findings: set[Finding],
) -> None:
    """
    Extract external URL references from item doc
    """
    for json_path, url in walk_json(document):
        hostname = hostname_from_url(url)

        if not hostname:
            continue

        if host_is_internal(hostname, internal_hosts):
            continue

        findings.add(
            Finding(
                hostname=hostname,
                url=url,
                item_id=item.id,
                item_title=item.title or "",
                item_type=item.type or "",
                source=source,
                json_path=json_path,
            )
        )


# outputs
def write_outputs(findings: set[Finding]) -> None:
    """
    write a preliminary allowlist and supporting data
    """
    OUTPUT_DIRECTORY.mkdir(parents=True, exist_ok=True)

    evidence_file = OUTPUT_DIRECTORY / "proxy_host_evidence.csv"
    hosts_file = OUTPUT_DIRECTORY / "allowed_proxy_hosts.json"

    sorted_findings = sorted(
        findings,
        key=lambda finding: (
            finding.hostname,
            finding.item_title.casefold(),
            finding.item_id,
            finding.source,
            finding.json_path,
            finding.url,
        ),
    )

    hostnames = sorted({finding.hostname for finding in sorted_findings})

    with evidence_file.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "hostname",
                "url",
                "item_id",
                "item_title",
                "item_type",
                "source",
                "json_path",
            ],
        )
        writer.writeheader()
        writer.writerows(asdict(finding) for finding in sorted_findings)

    with hosts_file.open("w", encoding="utf-8") as handle:
        json.dump(
            {
                "allowedProxyHosts": hostnames,
            },
            handle,
            indent=2,
        )
        handle.write("\n")

    print()
    print(f"Distinct external hosts: {len(hostnames)}")
    print(f"Evidence records:        {len(sorted_findings)}")
    print()
    print(f"Allowed hosts: {hosts_file}")
    print(f"Evidence:      {evidence_file}")


# main function
def main() -> int:
    print(f"Opening age-oauth connection: {CONNECTION}")

    gis = get_gis(connection=CONNECTION)

    portal_url = gis.url.rstrip("/")
    portal_host = hostname_from_url(portal_url)
    username = gis.users.me.username

    internal_hosts = set(INTERNAL_HOSTS)

    # Always exclude the Portal's own hostname.
    if portal_host:
        internal_hosts.add(portal_host)

    print(f"Portal: {portal_url}")
    print(f"User:   {username}")
    print("Searching organization content...")

    items = all_organization_items(gis)

    print(f"Found {len(items)} items.")

    findings: set[Finding] = set()

    for item_number, item in enumerate(items, start=1):
        if item_number == 1 or item_number % 100 == 0:
            print(f"Scanning item {item_number} of {len(items)}...")

        # Top-level properties include service item URLs, homepage URLs,
        # documentation references, descriptions, and other metadata.
        inspect_document(
            item_properties(item),
            item=item,
            source="item-properties",
            internal_hosts=internal_hosts,
            findings=findings,
        )

        # Item data contains Web Map operational layers, basemaps, tables,
        # app configurations, Web Scenes, dashboards, StoryMaps, and similar
        # JSON documents.
        try:
            data = item.get_data(try_json=True)
        except Exception as exc:
            print(
                f"Warning: could not read item data for "
                f"{item.id} ({item.title}): {exc}"
            )
            continue

        if data is not None:
            inspect_document(
                data,
                item=item,
                source="item-data",
                internal_hosts=internal_hosts,
                findings=findings,
            )

    write_outputs(findings)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())