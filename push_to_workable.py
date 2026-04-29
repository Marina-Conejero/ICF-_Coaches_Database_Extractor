"""
Bulk push from Airtable → Workable.

Triggered via repository_dispatch from an Airtable Automation. Reads every
Coach with `Marked for Push = true` (across all briefs) and pushes each one
to Workable, with email-based dedup against existing Workable candidates.

Updates Airtable with Workable ID / status, appends to relevant briefs'
Coaches Pushed link, writes a Workable Sync Log entry per coach, and posts
a Slack summary at the end.

Auth (env vars):
  AIRTABLE_PAT          — same as scraper
  WORKABLE_API_KEY      — Bearer token for Workable API
  SLACK_WEBHOOK_URL     — optional, posts summary if set
  GITHUB_RUN_URL        — optional, included in Slack message for traceability

Rate limits respected:
  Airtable: 5 req/sec per base — we sleep 0.25s between updates
  Workable: 10 req/sec per token — we don't approach this with sequential pushes
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

# ---------------------------------------------------------------------------
# Config — base IDs, Workable target. Same constants the rest of the project uses.
# ---------------------------------------------------------------------------

BASE_ID = "appKgfCoWcqDKhcnw"
COACHES_TABLE = "tbltOjxlouH6oowRW"
SCRAPE_RUNS_TABLE = "tbl5RQqn9oUDiGeHX"
SEARCH_BRIEFS_TABLE = "tblOuwNY72dpqdzds"
SYNC_LOG_TABLE = "tblqDYXa4Ganf2rOE"

WORKABLE_SUBDOMAIN = "samacoaching"
WORKABLE_BASE = f"https://{WORKABLE_SUBDOMAIN}.workable.com/spi/v3"
JOB_SHORTCODE = "1B6738BB2C"

AIRTABLE_BASE_API = f"https://api.airtable.com/v0/{BASE_ID}"
AIRTABLE_RATE_SLEEP = 0.25

EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")


# ---------------------------------------------------------------------------
# Lightweight clients
# ---------------------------------------------------------------------------

def airtable_session(pat: str) -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "Authorization": f"Bearer {pat}",
        "Content-Type": "application/json",
    })
    return s


def workable_session(token: str) -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    })
    return s


def airtable_list_all(s: requests.Session, table_id: str,
                      formula: str | None = None,
                      fields: list[str] | None = None) -> list[dict]:
    out: list[dict] = []
    url = f"{AIRTABLE_BASE_API}/{table_id}"
    params: dict = {"pageSize": 100}
    if formula:
        params["filterByFormula"] = formula
    if fields:
        params["fields[]"] = fields
    while True:
        r = s.get(url, params=params)
        r.raise_for_status()
        data = r.json()
        out.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            return out
        params["offset"] = offset
        time.sleep(AIRTABLE_RATE_SLEEP)


def airtable_update(s: requests.Session, table_id: str,
                    record_id: str, fields: dict) -> dict:
    url = f"{AIRTABLE_BASE_API}/{table_id}/{record_id}"
    r = s.patch(url, json={"fields": fields, "typecast": True})
    if r.status_code >= 400:
        raise RuntimeError(f"Airtable update failed: {r.status_code} {r.text}")
    return r.json()


def airtable_create(s: requests.Session, table_id: str, fields: dict) -> dict:
    url = f"{AIRTABLE_BASE_API}/{table_id}"
    r = s.post(url, json={"records": [{"fields": fields}], "typecast": True})
    if r.status_code >= 400:
        raise RuntimeError(f"Airtable create failed: {r.status_code} {r.text}")
    return r.json()["records"][0]


def airtable_get(s: requests.Session, table_id: str, record_id: str,
                 fields: list[str] | None = None) -> dict | None:
    url = f"{AIRTABLE_BASE_API}/{table_id}/{record_id}"
    params: dict = {}
    if fields:
        params["fields[]"] = fields
    r = s.get(url, params=params)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json()


# ---------------------------------------------------------------------------
# Workable operations
# ---------------------------------------------------------------------------

def workable_find_by_email(s: requests.Session, email: str) -> dict | None:
    url = f"{WORKABLE_BASE}/candidates"
    r = s.get(url, params={"email": email})
    if r.status_code >= 400:
        raise RuntimeError(f"Workable lookup failed: {r.status_code} {r.text}")
    data = r.json()
    candidates = data.get("candidates") or []
    return candidates[0] if candidates else None


def workable_create_candidate(s: requests.Session,
                              shortcode: str, payload: dict) -> tuple[int, dict | str]:
    """Returns (status_code, response_data_or_text)."""
    url = f"{WORKABLE_BASE}/jobs/{shortcode}/candidates"
    r = s.post(url, json=payload)
    if r.status_code >= 400:
        return r.status_code, r.text
    return r.status_code, r.json()


# ---------------------------------------------------------------------------
# Payload building
# ---------------------------------------------------------------------------

def coach_to_workable_payload(coach: dict, lookup: dict) -> dict:
    """Translate an Airtable coach record into a Workable candidate payload.

    `lookup` is a dict of cached Airtable lookups for linked records:
      lookup["countries"] = { record_id: {"name": ..., "code": ...} }
    """
    f = coach["fields"]
    first = f.get("First Name") or "Coach"
    last  = f.get("Last Name") or ""
    email = (f.get("Email") or "").strip().lower()
    headline = f.get("Headline") or f"{first} {last}".strip()

    def listify(v):
        if v is None:
            return []
        if isinstance(v, list):
            return [str(x.get("name") if isinstance(x, dict) else x) for x in v if x]
        return [s.strip() for s in str(v).split(",") if s.strip()]

    country_links = f.get("Country") or []
    country_names = []
    for cid in country_links:
        info = lookup["countries"].get(cid)
        if info:
            country_names.append(info["name"])

    credentials = listify(f.get("Credentials"))
    languages = listify(f.get("Fluent Languages"))
    themes = f.get("Coaching Themes") or ""
    industries = f.get("Industry Sectors") or ""
    methods = listify(f.get("Coaching Methods"))
    type_of_client = listify(f.get("Type of Client"))
    level_of_client = listify(f.get("Level of Client"))
    org_types = listify(f.get("Org Client Types"))

    city = f.get("City") or ""
    state = f.get("State Province") or ""
    countries_str = ", ".join(country_names)

    summary_lines = [
        headline,
        f"Serves: {countries_str}" if countries_str else "",
        f"Based in: {', '.join([p for p in [city, state, country_names[0] if country_names else ''] if p])}",
        f"ICF Credentials: {', '.join(credentials)}" if credentials else "",
        f"Languages: {', '.join(languages)}" if languages else "",
        f"Coaching focus: {themes}" if themes else "",
        f"Industry experience: {industries}" if industries else "",
        f"Typical client level: {', '.join(level_of_client)}" if level_of_client else "",
        f"Coaching methods: {', '.join(methods)}" if methods else "",
        f"Org client types: {', '.join(org_types)}" if org_types else "",
        f"Type of client: {', '.join(type_of_client)}" if type_of_client else "",
    ]
    summary = "\n\n".join(line for line in summary_lines if line)

    tags = ["Sama Coach"] + country_names + credentials

    payload = {
        "candidate": {
            "name": f"{first} {last}".strip() or email,
            "firstname": first,
            "lastname": last,
            "email": email,
            "headline": headline,
            "summary": summary,
            "address": ", ".join(p for p in [city, state, countries_str] if p),
            "phone": f.get("Phone") or "",
            "tags": list(dict.fromkeys(t for t in tags if t)),  # dedup preserve order
            "social_profiles": [],
        }
    }
    if f.get("LinkedIn"):
        payload["candidate"]["social_profiles"].append({
            "type": "linkedin",
            "url": f["LinkedIn"],
            "name": "LinkedIn",
        })
    return payload


# ---------------------------------------------------------------------------
# Per-coach processing
# ---------------------------------------------------------------------------

def write_sync_log(s: requests.Session, coach_id: str, action: str,
                   result: str, code: int | None = None,
                   body: str | None = None, error: str | None = None) -> None:
    fields = {
        "Sync ID": f"{datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')}__{coach_id}__{action}",
        "Coach": [coach_id],
        "Action": action,
        "Triggered At": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "Triggered By": "GitHub Actions push_to_workable.py",
        "Result": result,
    }
    if code is not None:
        fields["Workable Response Code"] = code
    if body:
        fields["Workable Response Body"] = str(body)[:5000]
    if error:
        fields["Error Details"] = str(error)[:5000]
    try:
        airtable_create(s, SYNC_LOG_TABLE, fields)
    except Exception as exc:
        print(f"WARN: sync log write failed: {exc}", file=sys.stderr)


def append_coach_to_briefs_pushed(s: requests.Session, coach_id: str,
                                  brief_ids: list[str]) -> None:
    """Add coach_id to each Brief's Coaches Pushed (without removing existing)."""
    for bid in brief_ids:
        try:
            brief = airtable_get(s, SEARCH_BRIEFS_TABLE, bid, fields=["Coaches Pushed"])
            if not brief:
                continue
            existing = [c["id"] if isinstance(c, dict) else c
                        for c in (brief["fields"].get("Coaches Pushed") or [])]
            if coach_id in existing:
                continue
            airtable_update(s, SEARCH_BRIEFS_TABLE, bid, {
                "Coaches Pushed": existing + [coach_id]
            })
            time.sleep(AIRTABLE_RATE_SLEEP)
        except Exception as exc:
            print(f"WARN: linking coach {coach_id} to brief {bid}: {exc}",
                  file=sys.stderr)


def process_coach(coach: dict,
                  airtable: requests.Session,
                  workable: requests.Session,
                  lookup: dict) -> tuple[str, str]:
    """Returns (status, reason). status is one of: pushed, duplicate, error.
    reason is a human-readable explanation (used for Slack), empty for non-errors."""
    coach_id = coach["id"]
    f = coach["fields"]
    email = (f.get("Email") or "").strip().lower()
    if not email or not EMAIL_RE.match(email):
        airtable_update(airtable, COACHES_TABLE, coach_id, {
            "Marked for Push": False,
            "Push Status": "Error",
            "Push Error Details": "Invalid or missing email",
        })
        write_sync_log(airtable, coach_id, "Push", "Error",
                       error="Invalid email")
        return "error", "Invalid or missing email"

    # Respect Do Not Contact
    if f.get("Do Not Contact"):
        airtable_update(airtable, COACHES_TABLE, coach_id, {
            "Marked for Push": False,
            "Push Status": "Error",
            "Push Error Details": "Do Not Contact flag set",
        })
        write_sync_log(airtable, coach_id, "Skip Duplicate", "Error",
                       error="Do Not Contact")
        return "error", "Do Not Contact flag set"

    # Already flagged In Workable + has ID — clear and skip
    if f.get("In Workable") and f.get("Workable ID"):
        airtable_update(airtable, COACHES_TABLE, coach_id, {
            "Marked for Push": False,
            "Push Status": "Already in Workable",
        })
        write_sync_log(airtable, coach_id, "Skip Duplicate", "Duplicate Skipped",
                       body="Already flagged In Workable")
        return "duplicate", ""

    # Workable email lookup — defensive dedup
    try:
        existing = workable_find_by_email(workable, email)
    except Exception as exc:
        reason = f"Lookup failed: {exc}"
        airtable_update(airtable, COACHES_TABLE, coach_id, {
            "Push Status": "Error",
            "Push Error Details": reason,
        })
        write_sync_log(airtable, coach_id, "Push", "Error", error=str(exc))
        return "error", reason

    if existing:
        airtable_update(airtable, COACHES_TABLE, coach_id, {
            "Marked for Push": False,
            "In Workable": True,
            "Workable ID": str(existing.get("id", "")),
            "Workable Stage": existing.get("stage"),
            "Workable Stage Kind": existing.get("stage_kind"),
            "Workable Profile URL": existing.get("profile_url"),
            "Push Status": "Already in Workable",
        })
        write_sync_log(airtable, coach_id, "Skip Duplicate", "Duplicate Skipped",
                       code=200, body=f"Existing Workable id {existing.get('id')}")
        return "duplicate", ""

    # Build & POST
    payload = coach_to_workable_payload(coach, lookup)
    code, body = workable_create_candidate(workable, JOB_SHORTCODE, payload)

    if code >= 400:
        reason = f"HTTP {code}: {str(body)[:300]}"
        airtable_update(airtable, COACHES_TABLE, coach_id, {
            "Push Status": "Error",
            "Push Error Details": f"HTTP {code}: {str(body)[:500]}",
        })
        write_sync_log(airtable, coach_id, "Push", "Error",
                       code=code, body=str(body), error=f"HTTP {code}")
        return "error", reason

    new = body["candidate"] if isinstance(body, dict) and "candidate" in body else None
    if not new or not new.get("id"):
        reason = "Workable accepted but returned no id"
        airtable_update(airtable, COACHES_TABLE, coach_id, {
            "Push Status": "Error",
            "Push Error Details": reason,
        })
        write_sync_log(airtable, coach_id, "Push", "Error",
                       code=code, body=str(body), error="No candidate id")
        return "error", reason

    workable_id = str(new["id"])
    airtable_update(airtable, COACHES_TABLE, coach_id, {
        "Marked for Push": False,
        "In Workable": True,
        "Workable ID": workable_id,
        "Workable Profile URL": new.get("profile_url"),
        "Workable Stage": new.get("stage"),
        "Workable Stage Kind": new.get("stage_kind"),
        "Pushed From Airtable": True,
        "Pushed Date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "Push Status": "Pushed",
        "Push Error Details": "",
    })

    # Link to all the briefs this coach matched
    brief_links = f.get("Briefs Matched") or []
    brief_ids = [b["id"] if isinstance(b, dict) else b for b in brief_links]
    append_coach_to_briefs_pushed(airtable, coach_id, brief_ids)

    write_sync_log(airtable, coach_id, "Push", "Success",
                   code=code, body=f"Workable id {workable_id}")
    return "pushed", ""


# ---------------------------------------------------------------------------
# Slack summary
# ---------------------------------------------------------------------------

def post_slack_summary(stats: dict, failures: list[dict]) -> None:
    webhook = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook:
        return
    run_url = os.environ.get("GITHUB_RUN_URL", "")
    base_url = f"https://airtable.com/{BASE_ID}"

    lines = [
        f"*Workable push complete*",
        "",
        f"✅ Pushed: *{stats['pushed']}*",
        f"🟡 Already in Workable: *{stats['duplicate']}*",
        f"❌ Errors: *{stats['error']}*",
    ]
    if failures:
        lines.append("")
        lines.append("*Failures (first 10):*")
        for fail in failures[:10]:
            email = fail.get("email", "?")
            reason = (fail.get("reason") or "")[:200]
            lines.append(f"• `{email}` — {reason}")
    if run_url:
        lines.append("")
        lines.append(f"<{run_url}|View workflow run> · <{base_url}|Open Airtable>")
    else:
        lines.append("")
        lines.append(f"<{base_url}|Open Airtable>")

    try:
        requests.post(webhook, json={"text": "\n".join(lines)}, timeout=10)
    except Exception as exc:
        print(f"WARN: Slack notification failed: {exc}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    pat = os.environ.get("AIRTABLE_PAT")
    workable_token = os.environ.get("WORKABLE_API_KEY")
    if not pat:
        sys.exit("ERROR: AIRTABLE_PAT env var not set.")
    if not workable_token:
        sys.exit("ERROR: WORKABLE_API_KEY env var not set.")

    airtable = airtable_session(pat)
    workable = workable_session(workable_token)

    # Load Countries lookup once (used for tag/payload composition)
    countries = airtable_list_all(airtable, "tblaaQcQb9D8yWS0c",
                                  fields=["Country Name", "ICF Code"])
    lookup = {"countries": {}}
    for c in countries:
        lookup["countries"][c["id"]] = {
            "name": c["fields"].get("Country Name", ""),
            "code": c["fields"].get("ICF Code", ""),
        }

    # Find the work to do
    formula = "AND({Marked for Push} = TRUE(), {Push Status} != 'Pushed')"
    coaches = airtable_list_all(airtable, COACHES_TABLE, formula=formula)
    print(f"\n=== {len(coaches)} coaches marked for push ===\n")

    if not coaches:
        post_slack_summary({"pushed": 0, "duplicate": 0, "error": 0}, [])
        return

    stats = {"pushed": 0, "duplicate": 0, "error": 0}
    failures: list[dict] = []
    started = time.time()

    for i, coach in enumerate(coaches, 1):
        email = (coach["fields"].get("Email") or "").strip().lower() or "?"
        try:
            result, reason = process_coach(coach, airtable, workable, lookup)
        except Exception as exc:
            print(f"  [{i}/{len(coaches)}] {email}: ERROR ({exc})", file=sys.stderr)
            stats["error"] += 1
            failures.append({"email": email, "reason": str(exc)[:300]})
            continue

        stats[result] += 1
        symbol = {"pushed": "✓", "duplicate": "↻", "error": "✗"}[result]
        print(f"  [{i}/{len(coaches)}] {email}: {symbol} {result}"
              + (f" — {reason}" if reason else ""))
        if result == "error":
            failures.append({"email": email, "reason": reason or "(no detail)"})

        time.sleep(AIRTABLE_RATE_SLEEP)

    duration = round(time.time() - started, 1)
    print(f"\n=== Done in {duration}s ===")
    print(json.dumps(stats, indent=2))

    post_slack_summary(stats, failures)


if __name__ == "__main__":
    main()
