#!/usr/bin/env python3
"""
Name:           proxy_hosts.py
Purpose:        Report external host references across an ArcGIS Enterprise instance
Dependencies:   age-oauth, arcgis
Outputs:        proxy_host_evidence.csv | allowed_proxy_hosts.json | proxy_host_scan_errors.csv
"""

from __future__ import annotations

import csv
import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import urlparse, urlunparse

import requests
from age_oauth import get_gis
from arcgis.gis import GIS, Item


# global config
CONNECTION = "deepskyagdf"  # age-oauth connection label

OUTPUT_DIRECTORY = Path("proxy-host-audit")

# Trusted internal hosts
# Can be explicit (subdomain.hostname.tld)
# or implicit (.hostname.tld will ignore *.hostname.tld)
INTERNAL_HOSTS = {
    ".gisa.public",
}

REQUEST_TIMEOUT = 30
VERIFY_EXTERNAL_TLS = False


# data models
@dataclass(frozen=True)
class Finding:
    hostname: str
    url: str
    item_id: str
    item_title: str
    item_type: str
    source: str
    json_path: str


@dataclass(frozen=True)
class ScanError:
    item_id: str
    item_title: str
    source: str
    error: str


# url slicing
URL_PATTERN = re.compile(
    r"""https?://[^\s"'<>\\)\]}]+""",
    flags=re.IGNORECASE,
)

ARCGIS_SERVICE_PATTERN = re.compile(
    r"""^(?P<root>https?://.+?/(?:MapServer|FeatureServer|ImageServer))"""
    r"""(?:/\d+)?/?(?:\?.*)?$""",
    flags=re.IGNORECASE,
)


def extract_urls(text: str) -> set[str]:
    """
    Extract HTTP and HTTPS URLs from text
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

        urls.add(normalize_url(candidate))

    return urls


def normalize_url(url: str) -> str:
    """
    Normalize url scheme and host
    """
    parsed = urlparse(url)

    hostname = (parsed.hostname or "").lower().rstrip(".")
    port = parsed.port

    if port is None:
        netloc = hostname
    else:
        netloc = f"{hostname}:{port}"

    return urlunparse(
        (
            parsed.scheme.lower(),
            netloc,
            parsed.path,
            parsed.params,
            parsed.query,
            "",
        )
    )


def hostname_from_url(url: str) -> str | None:
    """
    Return a lowercase hostname, drop port info if needed
    """
    try:
        hostname = urlparse(url).hostname
    except ValueError:
        return None

    if not hostname:
        return None

    return hostname.lower().rstrip(".")


def arcgis_service_root(url: str) -> str | None:
    """
    Convert ArcGIS service URL to its service root

    e.g. ../MapServer/0 to /MapServer
    """
    match = ARCGIS_SERVICE_PATTERN.match(url.rstrip("/"))

    if not match:
        return None

    return match.group("root").rstrip("/")


def host_is_internal(hostname: str, internal_hosts: set[str]) -> bool:
    """
    Check hostnames and DNS suffix rules
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


# inspect json
def walk_json(value: Any, path: str = "$") -> Iterator[tuple[str, str]]:
    """
    Yield JSON path and URL for every URL found recursively
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


def inspect_document(
    document: Any,
    item: Item,
    source: str,
    internal_hosts: set[str],
    findings: set[Finding],
) -> set[str]:
    """
    Inspect an arbitrary JSON-compatible document,
    return any ArcGIS REST service roots found in the doc so
    service and layer definitions can be inspected separately.
    """
    discovered_services: set[str] = set()

    for json_path, url in walk_json(document):
        hostname = hostname_from_url(url)

        if not hostname:
            continue

        service_root = arcgis_service_root(url)

        if service_root:
            discovered_services.add(service_root)

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

    return discovered_services


# Portal interrogation
def all_organization_items(gis: GIS) -> list[Item]:
    """
    Return organization content visible to the authenticated user (should be run as admin)
    """
    organization_id = gis.properties.id

    return gis.content.search(
        query=f"orgid:{organization_id}",
        max_items=-1,
        outside_org=False,
    )


def item_properties(item: Item) -> dict[str, Any]:
    """
    Convert ArcGIS API Item to dict
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


def request_arcgis_json(
    session: requests.Session,
    url: str,
    token: str | None,
) -> dict[str, Any]:
    """
    Read ArcGIS REST resource as JSON
    """
    params = {"f": "json"}

    if token:
        params["token"] = token

    response = session.get(
        url,
        params=params,
        timeout=REQUEST_TIMEOUT,
        verify=VERIFY_EXTERNAL_TLS,
    )
    response.raise_for_status()

    payload = response.json()

    if not isinstance(payload, dict):
        raise TypeError(f"Expected a JSON object from {url}")

    if "error" in payload:
        error = payload["error"]

        raise RuntimeError(
            f"ArcGIS REST error {error.get('code')}: "
            f"{error.get('message')}; "
            f"{error.get('details', [])}"
        )

    return payload


def inspect_arcgis_service(
    service_url: str,
    item: Item,
    session: requests.Session,
    portal_token: str | None,
    internal_hosts: set[str],
    findings: set[Finding],
    errors: list[ScanError],
    scanned_services: set[str],
) -> None:
    """
    Inspect ArcGIS service root and advertised layers/tables
    """
    service_url = service_url.rstrip("/")

    if service_url in scanned_services:
        return

    scanned_services.add(service_url)

    try:
        service_definition = request_arcgis_json(
            session=session,
            url=service_url,
            token=portal_token,
        )
    except Exception as exc:
        errors.append(
            ScanError(
                item_id=item.id,
                item_title=item.title or "",
                source=service_url,
                error=str(exc),
            )
        )
        return

    inspect_document(
        document=service_definition,
        item=item,
        source=f"service-definition:{service_url}",
        internal_hosts=internal_hosts,
        findings=findings,
    )

    for collection_name in ("layers", "tables"):
        entries = service_definition.get(collection_name, [])

        if not isinstance(entries, list):
            continue

        for entry in entries:
            if not isinstance(entry, dict):
                continue

            resource_id = entry.get("id")

            if resource_id is None:
                continue

            resource_url = f"{service_url}/{resource_id}"

            try:
                resource_definition = request_arcgis_json(
                    session=session,
                    url=resource_url,
                    token=portal_token,
                )

                inspect_document(
                    document=resource_definition,
                    item=item,
                    source=f"{collection_name[:-1]}-definition:{resource_url}",
                    internal_hosts=internal_hosts,
                    findings=findings,
                )

            except Exception as exc:
                errors.append(
                    ScanError(
                        item_id=item.id,
                        item_title=item.title or "",
                        source=resource_url,
                        error=str(exc),
                    )
                )


# outputs
def write_outputs(
    findings: set[Finding],
    errors: list[ScanError],
) -> None:
    """
    write evidence, candidate host config and errors
    """
    OUTPUT_DIRECTORY.mkdir(parents=True, exist_ok=True)

    evidence_file = OUTPUT_DIRECTORY / "proxy_host_evidence.csv"
    hosts_file = OUTPUT_DIRECTORY / "allowed_proxy_hosts.json"
    errors_file = OUTPUT_DIRECTORY / "proxy_host_scan_errors.csv"

    sorted_findings = sorted(
        findings,
        key=lambda finding: (
            finding.hostname,
            finding.item_title.casefold(),
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
            {"allowedProxyHosts": hostnames},
            handle,
            indent=2,
        )
        handle.write("\n")

    with errors_file.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "item_id",
                "item_title",
                "source",
                "error",
            ],
        )
        writer.writeheader()
        writer.writerows(asdict(error) for error in errors)

    print()
    print(f"Distinct external hosts: {len(hostnames)}")
    print(f"Evidence records:        {len(sorted_findings)}")
    print(f"Scan errors:             {len(errors)}")
    print()
    print(f"Evidence: {evidence_file}")
    print(f"Hosts:    {hosts_file}")
    print(f"Errors:   {errors_file}")


# main function
def main() -> int:
    print(f"Opening age-oauth connection: {CONNECTION}")

    gis = get_gis(connection=CONNECTION)

    username = gis.users.me.username
    portal_url = gis.url.rstrip("/")
    portal_host = hostname_from_url(portal_url)

    internal_hosts = set(INTERNAL_HOSTS)

    if portal_host:
        internal_hosts.add(portal_host)

    print(f"Portal: {portal_url}")
    print(f"User:   {username}")
    print("Searching organization content...")

    items = all_organization_items(gis)

    print(f"Found {len(items)} items.")

    findings: set[Finding] = set()
    errors: list[ScanError] = []
    scanned_services: set[str] = set()
    session = requests.Session()

    # age-oauth supplied the GIS and maintains access token lifecycle
    # Current token is reused for secured federated REST requests
    portal_token = getattr(gis._con, "token", None)

    for item_number, item in enumerate(items, start=1):
        if item_number == 1 or item_number % 100 == 0:
            print(f"Scanning item {item_number} of {len(items)}...")

        service_urls = inspect_document(
            document=item_properties(item),
            item=item,
            source="item-properties",
            internal_hosts=internal_hosts,
            findings=findings,
        )

        try:
            data = item.get_data(try_json=True)

            if data is not None:
                service_urls |= inspect_document(
                    document=data,
                    item=item,
                    source="item-data",
                    internal_hosts=internal_hosts,
                    findings=findings,
                )

        except Exception as exc:
            errors.append(
                ScanError(
                    item_id=item.id,
                    item_title=item.title or "",
                    source="item-data",
                    error=str(exc),
                )
            )

        item_url = getattr(item, "url", None)

        if isinstance(item_url, str):
            root = arcgis_service_root(item_url)

            if root:
                service_urls.add(root)

        for service_url in sorted(service_urls):
            inspect_arcgis_service(
                service_url=service_url,
                item=item,
                session=session,
                portal_token=portal_token,
                internal_hosts=internal_hosts,
                findings=findings,
                errors=errors,
                scanned_services=scanned_services,
            )

    write_outputs(findings, errors)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())