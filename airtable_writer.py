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
    """Backwards-compat wrapper — returns (city, country) from a Location."""
    city, _state, country = parse_location_v3(loc)
    return city, country


def parse_location_v3(loc: str | None,
                      known_country: str | None = None
                      ) -> tuple[str | None, str | None, str | None]:
    """Return (city, state_province, country) from a Location string.

    If `known_country` is supplied (the scraper always knows which country it
    just scraped), strip that country off the end of the Location string
    before parsing city/state. This handles multi-word countries like
    'Hong Kong', 'United States', etc., which a naive last-word approach
    would mangle.

    Examples:
      ('Munich, GERMANY', 'Germany')                 → ('Munich',     None,           'Germany')
      ('Toronto, ON CANADA', 'Canada')               → ('Toronto',    'ON',           'Canada')
      ('Tsimshatsui, Kowloon HONG KONG', 'Hong Kong')→ ('Tsimshatsui','Kowloon',      'Hong Kong')
      ('Lismore, Co Waterford IRELAND', 'Ireland')   → ('Lismore',    'Co Waterford', 'Ireland')
    """
    if not loc:
        return None, None, None
    s = loc.strip()

    # If we know the country, strip it from the end (case-insensitive)
    country_used = known_country
    if known_country:
        pattern = re.compile(re.escape(known_country) + r'\s*$', re.IGNORECASE)
        s = pattern.sub("", s).strip().rstrip(",").strip()

    parts = [p.strip() for p in s.split(",")]
    if not parts or (len(parts) == 1 and not parts[0]):
        return None, None, country_used

    if not country_used:
        # Fallback to old logic when we don't have a known country
        if len(parts) == 1:
            return None, None, parts[0] or None
        city = parts[0] or None
        if len(parts) >= 3:
            state = parts[1] or None
            country = parts[-1] or None
        else:
            last = parts[-1]
            words = last.split()
            if len(words) <= 1:
                state = None
                country = last or None
            else:
                state = " ".join(words[:-1])
                country = words[-1]
        return city, state, country

    # Country known + already stripped → remaining is city + optional state
    city = parts[0] or None
    state = ", ".join(p for p in parts[1:] if p) or None
    return city, state, country_used


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


_COMMA_RE = re.compile(r",\s*")


def normalise_commas(raw: str | None) -> str | None:
    """Replace any sequence of comma + whitespace with ', '. Cosmetic — used
    for text fields that store comma-separated values (Industry Sectors,
    Coaching Themes, Positions Held, Degrees, etc.) so they read cleanly.
    """
    if not raw:
        return None
    s = str(raw).strip()
    if not s:
        return None
    return _COMMA_RE.sub(", ", s) or None


def parse_yesno(raw: str | None) -> bool:
    if not raw:
        return False
    return str(raw).strip().lower() in {"yes", "true", "1", "y"}


def normalise_email(raw: str | None) -> str | None:
    if not raw:
        return None
    s = str(raw).strip().lower()
    return s if EMAIL_RE.match(s) else None


def normalise_url(raw: str | None) -> str | None:
    """Strip ICF's 'Unspecified' placeholder. Return None for empty / placeholder."""
    if not raw:
        return None
    s = str(raw).strip()
    if not s or s.lower().endswith("unspecified") or "://unspecified" in s.lower():
        return None
    return s


def normalise_phone(raw: str | None, country_dialing_code: str | None = None) -> str | None:
    """Best-effort E.164-style normalisation.

    Examples (with country_dialing_code='353'):
      '+353 (0) 861043805'  → '+353861043805'
      '086 4011438'         → '+353864011438'
      '00353 86 6025584'    → '+353866025584'
      '87 6217522'          → '+353876217522'

    If we can't determine the country code, we keep the raw '+...' form when
    present, otherwise return the digits unchanged.
    """
    if not raw:
        return None
    s = str(raw).strip()
    if s.endswith(".0"):
        s = s[:-2]
    # Keep + and digits only.
    s = re.sub(r"[^\d+]", "", s)
    if not s:
        return None
    # 00<country> → +<country>
    if s.startswith("00"):
        s = "+" + s[2:]
    # If we have a country code and the number doesn't start with +
    if country_dialing_code and not s.startswith("+"):
        # If the leading digits already match the country code, just prepend +
        if s.startswith(country_dialing_code):
            s = "+" + s
        else:
            # Strip national trunk prefix (leading 0s) and prepend country code
            stripped = s.lstrip("0")
            s = "+" + country_dialing_code + stripped
    return s or None


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
        self._country_cache: dict[str, dict] | None = None

    @classmethod
    def from_env(cls) -> "AirtableWriter | None":
        pat = os.environ.get("AIRTABLE_PAT")
        if not pat:
            return None
        return cls(pat)

    # ----- internal helpers -----

    def _build_country_cache(self) -> None:
        """Build a name (lowercased) → {id, code} lookup for the Countries table.
        Aliases (USA, UK, Holland, etc.) point to the same canonical entry."""
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
        cache: dict[str, dict] = {}
        for rec in records:
            fields = rec.get("fields", {})
            name = fields.get("Country Name")
            code = fields.get("ICF Code")
            if name:
                cache[name.lower()] = {"id": rec["id"], "code": code}
        # Common aliases
        aliases = {
            "usa": "United States", "u.s.a.": "United States", "us": "United States",
            "united states of america": "United States", "america": "United States",
            "uk": "United Kingdom", "u.k.": "United Kingdom", "great britain": "United Kingdom",
            "britain": "United Kingdom", "england": "United Kingdom",
            "deutschland": "Germany", "holland": "Netherlands",
        }
        for alias, canonical in aliases.items():
            entry = cache.get(canonical.lower())
            if entry:
                cache[alias] = entry
        self._country_cache = cache

    def country_id_for(self, country_name: str | None) -> str | None:
        info = self.country_info(country_name)
        return info["id"] if info else None

    def country_info(self, country_name: str | None) -> dict | None:
        """Return {id, code} for a country name, or None if not found."""
        if not country_name:
            return None
        if self._country_cache is None:
            self._build_country_cache()
        return self._country_cache.get(country_name.strip().lower())

    def parse_location_smart(self, loc: str | None
                             ) -> tuple[str | None, str | None, str | None]:
        """Use the Countries cache to find the longest matching country at
        the end of the Location string. Handles multi-word countries
        ('Hong Kong', 'United States') AND avoids treating state codes
        ('ON CANADA') as part of the country.

        Returns (city, state, canonical_country_name).
        """
        if not loc:
            return None, None, None
        if self._country_cache is None:
            self._build_country_cache()

        s = str(loc).strip()
        words = s.split()

        # Try longest country name first (4 words → 1 word)
        for n in range(4, 0, -1):
            if len(words) < n:
                continue
            candidate = " ".join(words[-n:]).strip().rstrip(",").strip()
            entry = self._country_cache.get(candidate.lower())
            if entry:
                # Strip country off; what remains is "City, optional State"
                remaining = " ".join(words[:-n]).strip().rstrip(",").strip()
                parts = [p.strip() for p in remaining.split(",") if p.strip()]
                city = parts[0] if parts else None
                state = ", ".join(parts[1:]) if len(parts) > 1 else None
                # Look up the canonical (proper case) country name
                canonical_country = None
                for k, v in self._country_cache.items():
                    if v == entry:
                        canonical_country = k.title() if k.islower() else k
                        break
                return city, state, canonical_country or candidate.title()

        # No country match in lookup — fall back to naive parsing
        parts = [p.strip() for p in s.split(",")]
        if len(parts) == 1:
            return None, None, parts[0] or None
        city = parts[0] or None
        last = parts[-1]
        last_words = last.split()
        if len(last_words) == 1:
            return city, None, last
        return city, " ".join(last_words[:-1]), last_words[-1]

    # ----- public API -----

    def start_scrape_run(self, run_label: str, params: dict) -> str:
        """Create a Scrape Run record. Return its id.

        If params['brief_id'] is set, also links the Scrape Run to that
        Search Brief — Airtable mirrors this on the Brief's Scrape Run field
        automatically, so Brief → Coaches Matched can be populated by the
        Brief Link Coaches Automation when the run completes.
        """
        country_ids = []
        for country in params.get("countries", []):
            cid = self.country_id_for(country.get("name"))
            if cid:
                country_ids.append(cid)

        brief_id = params.get("brief_id")
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
                "Search Briefs": [brief_id] if brief_id else None,
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

    def upsert_coach(self, row: dict, scrape_run_id: str,
                     applied_credentials: list[str] | None = None) -> str:
        """Create or update a coach record from a scraper CSV row.

        applied_credentials: the credential filter values passed to the scrape
        (e.g. ['MCC']). Every coach in this scrape has these by definition, so
        we make sure they end up in the Credentials field even when the
        coach's displayed name doesn't mention them.

        Returns 'created' / 'updated' / 'skipped'."""
        email = normalise_email(row.get("Email"))
        if not email:
            return "skipped"

        first, last = split_name(row.get("Coach_Name"))
        # Parse the Location string to discover the coach's REAL country
        # (which may differ from the scrape filter — ICF's location filter
        # returns coaches who serve a region, not who live there). Use the
        # smart parser that knows about multi-word countries.
        city, state, country_from_loc = self.parse_location_smart(row.get("Location"))
        # Use the parsed country if it matched our lookup; else fall back
        # to the scrape param Country (still better than nothing).
        country_str = country_from_loc or row.get("Country")
        country_info = self.country_info(country_str)
        country_id = country_info["id"] if country_info else None
        country_code = country_info.get("code") if country_info else None

        icf_creds_raw, other_creds = split_credentials(row.get("Coach_Name") or row.get("Credentials"))
        icf_creds_set = set(icf_creds_raw)
        # ICF credential level (ACC/PCC/MCC) is mutually exclusive — a coach
        # has exactly ONE current level, even if their headline lists older
        # ones from when they progressed up. ACTC is an additive add-on.
        if applied_credentials and len(applied_credentials) == 1:
            # Single-credential filter — ICF's filter is authoritative
            single = applied_credentials[0].upper()
            if single in {"ACC", "PCC", "MCC"}:
                # Drop any other levels; keep ACTC if mentioned
                icf_creds_set = {single}
                if "ACTC" in icf_creds_raw:
                    icf_creds_set.add("ACTC")
            elif single == "ACTC":
                # Filter was ACTC — coach has ACTC plus possibly a level
                icf_creds_set.add("ACTC")
        elif applied_credentials and len(applied_credentials) > 1:
            # Multi-credential filter — coach has at least one of the
            # filtered levels. If headline mentions multiple, pick the
            # highest (MCC > PCC > ACC).
            levels_in_headline = icf_creds_set & {"ACC", "PCC", "MCC"}
            if levels_in_headline:
                if "MCC" in levels_in_headline:
                    chosen = "MCC"
                elif "PCC" in levels_in_headline:
                    chosen = "PCC"
                else:
                    chosen = "ACC"
                icf_creds_set = {chosen}
                if "ACTC" in icf_creds_raw:
                    icf_creds_set.add("ACTC")
            # If no level mentioned in headline, leave empty
            # (we can't disambiguate which of the multi-filter values applies)
        # No filter info → trust the headline
        icf_creds = sorted(icf_creds_set)

        fields: dict[str, Any] = {
            "Email": email,
            "First Name": first,
            "Last Name": last,
            "Phone": normalise_phone(row.get("Phone"), country_code),
            "Website": normalise_url(row.get("Website")),
            "City": city,
            "State Province": state,
            "Headline": row.get("Coach_Name") or None,
            "Credentials": icf_creds or None,
            "Other Certifications": ", ".join(other_creds) if other_creds else None,
            "Coaching Themes": normalise_commas(row.get("Coaching Themes")),
            "Coaching Methods": split_multiselect(row.get("Coaching Methods")) or None,
            "Fluent Languages": split_multiselect(row.get("Fluent Languages")) or None,
            "Type of Client": split_multiselect(row.get("Type of Client")) or None,
            "Org Client Types": split_multiselect(row.get("Organizational Client Types")) or None,
            "Industry Sectors": normalise_commas(row.get("Industry Sectors Coached")),
            "Level of Client": split_multiselect(row.get("Positions Held")) or None,
            "Coached Organizations": split_multiselect(row.get("Coached Organizations")) or None,
            "Positions Held": normalise_commas(row.get("Positions Held")),
            "Degrees": normalise_commas(row.get("Degrees")),
            "Rate": row.get("Rate") or None,
            "Fee Range": row.get("Fee Range") or None,
            "Willing to Relocate": parse_yesno(row.get("Willing to Relocate")),
            "Special Rates": split_multiselect(row.get("Special Rates")) or None,
            "Has Coach Skills Training Experience": parse_yesno(
                row.get("Has Prior Experience Delivering Coach Skills Training to Managers/Leaders")
            ),
            "Can Provide": split_multiselect(row.get("Can Provide")) or None,
            "ICF Profile URL": normalise_url(row.get("ICF Profile URL")),
            "Source": "ICF Scrape",
            "Scrape Run": [scrape_run_id],
            "Last Scraped At": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
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

            # Scrape Run is APPENDED, not replaced — preserves historical
            # provenance across repeat scrapes of the same coach.
            existing_runs = list(preserved.get("Scrape Run") or [])
            if scrape_run_id and scrape_run_id not in existing_runs:
                fields["Scrape Run"] = existing_runs + [scrape_run_id]
            else:
                fields.pop("Scrape Run", None)  # already linked, no-op

            # Credentials REPLACE (not accumulate) — the headline is the
            # source of truth for the coach's current ICF credential level.
            # Over-assignment from an earlier multi-credential filter run
            # gets corrected on next re-scrape.

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
