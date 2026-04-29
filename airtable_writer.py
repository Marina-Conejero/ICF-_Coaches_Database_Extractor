"""
Airtable write-back layer for the ICF scraper.

Streams scraped coaches directly into the "Coach Recruitment" base, deduplicating
by email. Creates a Scrape Run record at the start of every run so coaches are
linked to the run that captured them.

Used by scraper.py when AIRTABLE_PAT env var is set. If unset, the scraper falls
back to CSV-only output (handy for local dev / when you don't want to burn API
quota during testing).

Auth:
  AIRTABLE_PAT env var. Token needs:
    - data.records:read
    - data.records:write
    - schema.bases:read   (to look up Country records, field metadata)

Usage from scraper.py:
    writer = AirtableWriter.from_env()  # or None if env not set
    if writer:
        run_id = writer.start_scrape_run(run_label, params)
    ...
    for row in scraped_rows:
        if writer:
            writer.upsert_coach(row, run_id)
    ...
    if writer:
        writer.finish_scrape_run(run_id, status='Completed', summary=...)
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Any

import requests

BASE_ID = "appKgfCoWcqDKhcnw"
COACHES_TABLE = "tbltOjxlouH6oowRW"
SCRAPE_RUNS_TABLE = "tbl5RQqn9oUDiGeHX"
COUNTRIES_TABLE = "tblaaQcQb9D8yWS0c"
API_BASE = "https://api.airtable.com/v0"

ICF_REGEX = re.compile(r"\b(ACC|PCC|MCC|ACTC)\b", re.IGNORECASE)
EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")

RATE_LIMIT_SLEEP = 0.25


# ---------------------------------------------------------------------------
# Field mappers — turn a raw scraper CSV row dict into an Airtable Coaches record
# ---------------------------------------------------------------------------

def split_name(full: str | None) -> tuple[str | None, str | None]:
    if not full:
        return None, None
    s = re.sub(r"^(Mr\.|Mrs\.|Ms\.|Dr\.|Prof\.|Mx\.)\s+", "", full.strip(), flags=re.IGNORECASE)
    s = s.split(",")[0].strip()
    parts = s.split()
    if not parts:
        return None, None
    if len(parts) == 1:
        return parts[0], None
    return parts[0], " ".join(parts[1:])


def parse_location(loc: str | None) -> tuple[str | None, str | None]:
    """'Munich, GERMANY' → ('Munich', 'GERMANY'); 'Toronto, ON CANADA' → ('Toronto', 'CANADA')."""
    if not loc:
        return None, None
    parts = [p.strip() for p in loc.split(",")]
    if len(parts) == 1:
        return None, parts[0]
    city = parts[0]
    tail = parts[-1]
    country = tail.split()[-1] if " " in tail else tail
    return city, country


def split_credentials(raw: str | None) -> tuple[list[str], list[str]]:
    """Extract ICF credentials (ACC/PCC/MCC/ACTC); everything else → 'other'."""
    if not raw:
        return [], []
    icf: set[str] = set()
    other: list[str] = []
    for token in re.split(r",\s*", raw.strip()):
        if not token:
            continue
        upper = token.strip().upper()
        if upper in {"ACC", "PCC", "MCC", "ACTC"}:
            icf.add(upper)
            continue
        matches = ICF_REGEX.findall(token)
        if matches:
            icf.update(m.upper() for m in matches)
            residue = ICF_REGEX.sub("", token).strip(" ,;-")
            if residue:
                other.append(residue)
        else:
            other.append(token.strip())
    return sorted(icf), other


def split_multiselect(raw: str | None) -> list[str]:
    """Comma-split a string into a list of trimmed values."""
    if not raw:
        return []
    return [p.strip() for p in raw.split(",") if p.strip()]


def parse_yesno(raw: str | None) -> bool:
    if not raw:
        return False
    return str(raw).strip().lower() in {"yes", "true", "1", "y"}


def normalise_email(raw: str | None) -> str | None:
    if not raw:
        return None
    s = str(raw).strip().lower()
    return s if EMAIL_RE.match(s) else None


# ---------------------------------------------------------------------------
# Writer
# ---------------------------------------------------------------------------

class AirtableWriter:
    def __init__(self, pat: str, base_id: str = BASE_ID):
        self.base_id = base_id
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {pat}",
            "Content-Type": "application/json",
        })
        self._country_cache: dict[str, str] | None = None

    @classmethod
    def from_env(cls) -> "AirtableWriter | None":
        pat = os.environ.get("AIRTABLE_PAT")
        if not pat:
            return None
        return cls(pat)

    # ----- internal helpers -----

    def _build_country_cache(self) -> None:
        url = f"{API_BASE}/{self.base_id}/{COUNTRIES_TABLE}"
        records: list[dict] = []
        params: dict = {"pageSize": 100}
        while True:
            r = self.session.get(url, params=params)
            r.raise_for_status()
            data = r.json()
            records.extend(data.get("records", []))
            offset = data.get("offset")
            if not offset:
                break
            params["offset"] = offset
            time.sleep(RATE_LIMIT_SLEEP)
        cache: dict[str, str] = {}
        for rec in records:
            name = rec.get("fields", {}).get("Country Name")
            if name:
                cache[name.lower()] = rec["id"]
        # Common aliases
        aliases = {
            "usa": "United States", "u.s.a.": "United States", "us": "United States",
            "united states of america": "United States", "america": "United States",
            "uk": "United Kingdom", "u.k.": "United Kingdom", "great britain": "United Kingdom",
            "britain": "United Kingdom", "england": "United Kingdom",
            "deutschland": "Germany", "holland": "Netherlands",
        }
        for alias, canonical in aliases.items():
            target_id = cache.get(canonical.lower())
            if target_id:
                cache[alias] = target_id
        self._country_cache = cache

    def country_id_for(self, country_name: str | None) -> str | None:
        if not country_name:
            return None
        if self._country_cache is None:
            self._build_country_cache()
        norm = country_name.strip().lower()
        return self._country_cache.get(norm)

    # ----- public API -----

    def start_scrape_run(self, run_label: str, params: dict) -> str:
        """Create a Scrape Run record. Return its id."""
        country_ids = []
        for country in params.get("countries", []):
            cid = self.country_id_for(country.get("name"))
            if cid:
                country_ids.append(cid)
        body = {
            "fields": {
                "Run Label": run_label,
                "Triggered By": params.get("triggered_by", "GitHub Actions"),
                "Countries": country_ids,
                "Credentials Filter": params.get("credentials", []),
                "Languages Filter": ", ".join(params.get("languages", [])) or None,
                "Client Types Filter": ", ".join(params.get("coached_organizations", [])) or None,
                "Started At": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "Status": "Running",
                "GitHub Run URL": params.get("github_run_url"),
            }
        }
        body["fields"] = {k: v for k, v in body["fields"].items() if v is not None}
        url = f"{API_BASE}/{self.base_id}/{SCRAPE_RUNS_TABLE}"
        r = self.session.post(url, json={"records": [body], "typecast": True})
        if r.status_code >= 400:
            raise RuntimeError(f"Failed to create Scrape Run: {r.status_code} {r.text}")
        return r.json()["records"][0]["id"]

    def finish_scrape_run(self, run_id: str, status: str,
                          error_log: str | None = None) -> None:
        body = {
            "fields": {
                "Status": status,
                "Completed At": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            }
        }
        if error_log:
            body["fields"]["Error Log"] = error_log[:50000]
        url = f"{API_BASE}/{self.base_id}/{SCRAPE_RUNS_TABLE}/{run_id}"
        r = self.session.patch(url, json={"fields": body["fields"], "typecast": True})
        if r.status_code >= 400:
            print(f"WARN: failed to update Scrape Run {run_id}: {r.text}",
                  file=sys.stderr)

    def find_coach_by_email(self, email: str) -> dict | None:
        """Look up a single coach by email. Returns the record or None."""
        if not email:
            return None
        # Airtable filterByFormula needs the value escaped
        formula = f"LOWER({{Email}}) = '{email.lower()}'"
        url = f"{API_BASE}/{self.base_id}/{COACHES_TABLE}"
        r = self.session.get(url, params={
            "filterByFormula": formula, "maxRecords": 1
        })
        r.raise_for_status()
        records = r.json().get("records", [])
        return records[0] if records else None

    def upsert_coach(self, row: dict, scrape_run_id: str) -> str:
        """Create or update a coach record from a scraper CSV row.

        Returns 'created' / 'updated' / 'skipped'."""
        email = normalise_email(row.get("Email"))
        if not email:
            return "skipped"

        first, last = split_name(row.get("Coach_Name"))
        city, country_str = parse_location(row.get("Location"))
        if not country_str:
            country_str = row.get("Country")  # fallback to scrape param
        country_id = self.country_id_for(country_str)

        icf_creds, other_creds = split_credentials(row.get("Coach_Name") or row.get("Credentials"))

        fields: dict[str, Any] = {
            "Email": email,
            "First Name": first,
            "Last Name": last,
            "Phone": (row.get("Phone") or "").strip() or None,
            "Website": (row.get("Website") or "").strip() or None,
            "City": city,
            "Headline": row.get("Coach_Name") or None,
            "Credentials": icf_creds or None,
            "Other Certifications": ", ".join(other_creds) if other_creds else None,
            "Coaching Themes": row.get("Coaching Themes") or None,
            "Coaching Methods": split_multiselect(row.get("Coaching Methods")) or None,
            "Fluent Languages": split_multiselect(row.get("Fluent Languages")) or None,
            "Type of Client": split_multiselect(row.get("Type of Client")) or None,
            "Org Client Types": split_multiselect(row.get("Organizational Client Types")) or None,
            "Industry Sectors": row.get("Industry Sectors Coached") or None,
            "Level of Client": split_multiselect(row.get("Positions Held")) or None,
            "Coached Organizations": split_multiselect(row.get("Coached Organizations")) or None,
            "Positions Held": row.get("Positions Held") or None,
            "Degrees": row.get("Degrees") or None,
            "Rate": row.get("Rate") or None,
            "Fee Range": row.get("Fee Range") or None,
            "Willing to Relocate": parse_yesno(row.get("Willing to Relocate")),
            "Special Rates": split_multiselect(row.get("Special Rates")) or None,
            "Has Coach Skills Training Experience": parse_yesno(
                row.get("Has Prior Experience Delivering Coach Skills Training to Managers/Leaders")
            ),
            "Can Provide": split_multiselect(row.get("Can Provide")) or None,
            "Source": "ICF Scrape",
            "Scrape Run": [scrape_run_id],
        }
        if country_id:
            fields["Country"] = [country_id]

        # Drop None values
        fields = {k: v for k, v in fields.items() if v is not None and v != []}

        existing = self.find_coach_by_email(email)
        if existing:
            # Update — but DON'T clobber Workable fields if already set
            url = f"{API_BASE}/{self.base_id}/{COACHES_TABLE}/{existing['id']}"
            preserved = existing.get("fields", {})
            for k in (
                "In Workable", "Workable ID", "Workable Stage", "Workable Stage Kind",
                "Workable Disqualified", "Workable Disqualification Reason",
                "Workable Profile URL", "Pushed From Airtable", "Pushed Date",
                "Push Status",
            ):
                if k in preserved:
                    fields.pop(k, None)
            r = self.session.patch(url, json={"fields": fields, "typecast": True})
            if r.status_code >= 400:
                print(f"WARN: update failed for {email}: {r.text}", file=sys.stderr)
                return "skipped"
            return "updated"
        else:
            url = f"{API_BASE}/{self.base_id}/{COACHES_TABLE}"
            r = self.session.post(url, json={
                "records": [{"fields": fields}], "typecast": True
            })
            if r.status_code >= 400:
                print(f"WARN: create failed for {email}: {r.text}", file=sys.stderr)
                return "skipped"
            return "created"


__all__ = ["AirtableWriter"]
