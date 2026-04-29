"""
ICF Credentialed Coach Finder — parameterised scraper
======================================================

Refactor of CF_scraping.py + Main.py to:
  - Accept a JSON params dict instead of Streamlit text inputs
  - Iterate across multiple countries in one run
  - Run headless under GitHub Actions (no local Chrome required)
  - Support credential/location/language/coached-org filters Caitlin uses
  - Stream coach rows to a single aggregated CSV per run

Phase 2 (next session) will add Airtable write-back. For now this still
writes to CSV — the output is identical in shape to Konrad's old export
so downstream consumers stay compatible.

Usage:
  python3 scraper.py --params params.json

See params.example.json for the input shape.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from dataclasses import dataclass, field
from typing import Iterable

from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

from airtable_writer import AirtableWriter

# ---------------------------------------------------------------------------
# DOM element constants — captured from the live ICF directory April 2026.
# See sama_icf_filter_ids.md in memory for the full filter inventory.
# ---------------------------------------------------------------------------

ICF_SEARCH_URL = (
    "https://apps.coachingfederation.org/eweb/CCFDynamicPage.aspx"
    "?webcode=ccfsearch&site=icfapp"
)

CREDENTIAL_IDS = {
    "ACC": "credential-acc",
    "PCC": "credential-pcc",
    "MCC": "credential-mcc",
}

COACHED_ORG_IDS = {
    "Global/Multi-national": "coached-global",
    "Nonprofit/NGO": "coached-non-profit",
}

MODAL_BUTTON_IDS = {
    "language": "add-fluent-language",
    "location": "add-location",
}

MODAL_CONTAINER_IDS = {
    "language": "fluent-languages-modal",
    "location": "locations-modal",
}

PROFILE_FIELD_IDS = {
    "name": "coachName",
    "website": "webSiteLink",
    "email": "emailLink",
    "phone": "phoneLbl",
    "address": "addressLbl",
    "fee": "coachFee",
}

CARDS_CONTAINER_ID = "cards"
CARD_CLASS = "ui fluid link  card"
TABLE_CLASS = "ui.unstackable.very.basic.definition.table"

OUTPUT_HEADERS = [
    "Coach_Name", "Website", "Email", "Phone", "Location", "Rate",
    "Coaching Themes", "Coaching Methods", "Willing to Relocate",
    "Special Rates", "Fee Range", "Type of Client",
    "Organizational Client Types", "Coached Organizations",
    "Industry Sectors Coached", "Positions Held",
    "Has Prior Experience Delivering Coach Skills Training to Managers/Leaders",
    "Degrees", "Gender", "Age", "Fluent Languages", "Can Provide",
    # Run metadata appended by Runner:
    "Country", "Run_Label", "Scraped_At",
]


# ---------------------------------------------------------------------------
# Browser init
# ---------------------------------------------------------------------------

def init_browser(headless: bool = True) -> webdriver.Chrome:
    """Spin up a Chrome instance suitable for either local debug or CI."""
    options = webdriver.ChromeOptions()
    if headless:
        # 'new' headless mode is more reliable for modern sites.
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
    service = ChromeService(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)


# ---------------------------------------------------------------------------
# Filter helpers
# ---------------------------------------------------------------------------

def check_checkbox_ifnot(browser: webdriver.Chrome, element_id: str) -> None:
    """Toggle a checkbox to checked state if it isn't already."""
    try:
        cb = browser.find_element(By.XPATH, f"//input[@id='{element_id}']")
        if not cb.is_selected():
            browser.execute_script(
                f"document.getElementById('{element_id}').click();"
            )
    except Exception as exc:
        print(f"  warning: could not check {element_id!r}: {exc}", file=sys.stderr)


def apply_credential_filters(browser: webdriver.Chrome,
                             credentials: list[str]) -> None:
    """Check the requested ICF credential checkboxes (ACC/PCC/MCC)."""
    requested = {c.upper() for c in (credentials or [])}
    if not requested:
        # Default to all three if caller didn't specify.
        requested = set(CREDENTIAL_IDS.keys())
    for cred in requested:
        cb_id = CREDENTIAL_IDS.get(cred)
        if cb_id:
            check_checkbox_ifnot(browser, cb_id)
            print(f"  ✓ credential filter: {cred}")
        else:
            print(f"  warning: unknown credential {cred!r}", file=sys.stderr)


def apply_coached_org_filters(browser: webdriver.Chrome,
                              client_types: list[str]) -> None:
    """Check 'Coaches Global/Multi-national' / 'Nonprofit/NGO' checkboxes."""
    for ct in client_types or []:
        cb_id = COACHED_ORG_IDS.get(ct)
        if cb_id:
            check_checkbox_ifnot(browser, cb_id)
            print(f"  ✓ coached-org filter: {ct}")
        else:
            print(f"  warning: unknown coached-org {ct!r}", file=sys.stderr)


def apply_modal_filter(browser: webdriver.Chrome, kind: str,
                       values: list[str]) -> None:
    """Open a modal (language or location), tick each requested value, close it.

    The ICF directory uses `button[data-display='<value>']` inside each modal
    to represent selectable items — same pattern as Konrad's location code.
    """
    if not values:
        return
    btn_id = MODAL_BUTTON_IDS[kind]
    modal_id = MODAL_CONTAINER_IDS[kind]

    # Open the modal
    browser.execute_script(f"document.getElementById('{btn_id}').click();")
    time.sleep(2)

    selected_count = 0
    for value in values:
        try:
            item = browser.find_element(
                By.XPATH, f"//button[@data-display='{value}']"
            )
            item.click()
            selected_count += 1
            print(f"  ✓ {kind}: {value}")
        except Exception:
            print(f"  warning: could not find {kind} option {value!r}",
                  file=sys.stderr)

    # Close the modal via Semantic UI's modal-hide method
    browser.execute_script(f"$('#{modal_id}').modal('hide');")
    time.sleep(2)

    if selected_count == 0:
        print(f"  warning: no {kind} values were selected — filter not applied",
              file=sys.stderr)


# ---------------------------------------------------------------------------
# Data extraction (largely preserved from Konrad's original)
# ---------------------------------------------------------------------------

def get_inner_text(browser: webdriver.Chrome, element_id: str) -> str:
    """Return text content of an element by id, 'N/A' on miss."""
    try:
        elem = browser.find_element(By.ID, element_id)
        return elem.text if elem else "N/A"
    except Exception:
        return "N/A"


def get_table_data(browser: webdriver.Chrome) -> list[str]:
    """Pull the bottom-of-profile attribute table (themes, industries, etc.)."""
    try:
        element = browser.find_element(By.CLASS_NAME, TABLE_CLASS)
        rows = element.find_element(By.TAG_NAME, "tbody").find_elements(
            By.TAG_NAME, "tr"
        )
        out = []
        for row in rows:
            divs = row.find_elements(By.TAG_NAME, "div")
            value = ",".join(d.text for d in divs)
            out.append(value)
        return out
    except Exception:
        return []


def extract_profile_row(browser: webdriver.Chrome,
                        wait: WebDriverWait,
                        coach_value: str) -> list[str]:
    """Open a coach profile in a new tab, extract the row, close the tab."""
    browser.switch_to.new_window()
    browser.switch_to.window(browser.window_handles[-1])
    browser.execute_script(
        f"window.open('https://apps.coachingfederation.org/eweb/"
        f"CCFDynamicPage.aspx?webcode=ccfcoachprofileview"
        f"&coachcstkey={coach_value}')"
    )
    browser.close()
    browser.switch_to.window(browser.window_handles[-1])

    wait.until(EC.presence_of_element_located((By.ID, PROFILE_FIELD_IDS["name"])))

    row = [
        get_inner_text(browser, PROFILE_FIELD_IDS["name"]),
        get_inner_text(browser, PROFILE_FIELD_IDS["website"]),
        get_inner_text(browser, PROFILE_FIELD_IDS["email"]),
        get_inner_text(browser, PROFILE_FIELD_IDS["phone"]),
        get_inner_text(browser, PROFILE_FIELD_IDS["address"]),
        get_inner_text(browser, PROFILE_FIELD_IDS["fee"]),
    ] + get_table_data(browser)

    browser.close()
    browser.switch_to.window(browser.window_handles[-1])
    return row


def iterate_cards(browser: webdriver.Chrome, wait: WebDriverWait) -> list[list[str]]:
    """Walk every coach card on the current page, collect rows."""
    rows: list[list[str]] = []
    step = 1
    while True:
        try:
            card = browser.find_element(
                By.XPATH,
                f"//div[@class='{CARD_CLASS}'][position()={step}]"
            )
        except Exception:
            break
        try:
            check_input = card.find_element(By.TAG_NAME, "input")
            coach_value = check_input.get_attribute("value")
            row = extract_profile_row(browser, wait, coach_value)
            rows.append(row)
            wait.until(EC.presence_of_element_located((By.ID, CARDS_CONTAINER_ID)))
        except Exception as exc:
            print(f"    warning: card {step} failed: {exc}", file=sys.stderr)
        step += 1
    return rows


def iterate_pages(browser: webdriver.Chrome,
                  wait: WebDriverWait) -> Iterable[list[str]]:
    """Yield rows across every page of search results, paging via 'a.item'."""
    page = 1
    while True:
        if page > 1:
            try:
                current = browser.find_element(By.CSS_SELECTOR, "a.item.active")
                page_num = int(current.get_attribute("data-value"))
                next_link = browser.find_element(
                    By.XPATH,
                    f"//a[@class='item'][@data-value={page_num + 1}]"
                )
                if "disabled" in next_link.get_attribute("class"):
                    return
                next_link.click()
                time.sleep(2)
                wait.until(EC.presence_of_element_located((By.ID, CARDS_CONTAINER_ID)))
            except Exception:
                return
        rows = iterate_cards(browser, wait)
        for r in rows:
            yield r
        if not rows:
            return
        page += 1


# ---------------------------------------------------------------------------
# Driver: applies filters, then walks results.
# ---------------------------------------------------------------------------

@dataclass
class CountryParams:
    name: str          # Display name as used in ICF location modal
    code: str = ""     # Phone country code, used for phone-number cleanup
    icf_code: str = ""  # Optional ICF internal code (currently unused)


@dataclass
class RunParams:
    countries: list[CountryParams]
    credentials: list[str] = field(default_factory=list)        # ['ACC','PCC']
    languages: list[str] = field(default_factory=list)          # ['English','German']
    coached_organizations: list[str] = field(default_factory=list)  # ['Global/Multi-national']
    run_label: str = "scrape"
    output_path: str = "raw_data.csv"
    headless: bool = True
    page_load_wait: int = 30


def run_country(browser: webdriver.Chrome,
                country: CountryParams,
                params: RunParams) -> Iterable[list[str]]:
    """Run a single-country scrape; yield raw rows."""
    print(f"\n--- {country.name} ---")
    browser.get(ICF_SEARCH_URL)
    wait = WebDriverWait(browser, params.page_load_wait)
    wait.until(EC.presence_of_element_located((By.ID, "credential-acc")))
    wait.until(EC.presence_of_element_located((By.ID, "add-location")))

    apply_credential_filters(browser, params.credentials)
    apply_coached_org_filters(browser, params.coached_organizations)
    apply_modal_filter(browser, "location", [country.name])
    apply_modal_filter(browser, "language", params.languages)

    # Wait for results.
    try:
        wait.until(EC.presence_of_element_located((By.ID, CARDS_CONTAINER_ID)))
    except Exception:
        print(f"  no results for {country.name}")
        return

    yield from iterate_pages(browser, wait)


def Runner(params: RunParams) -> dict:
    """Entry point — runs the scrape across every country in params and
    streams rows to CSV. If AIRTABLE_PAT is set, ALSO writes to Airtable in
    real time, deduping by email. Returns a small summary."""
    output_dir = os.path.dirname(params.output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    # Optional Airtable writer.
    at_writer = AirtableWriter.from_env()
    scrape_run_id: str | None = None
    if at_writer:
        print("Airtable write-back: ENABLED")
        try:
            scrape_run_id = at_writer.start_scrape_run(
                params.run_label,
                {
                    "countries": [{"name": c.name, "code": c.code} for c in params.countries],
                    "credentials": params.credentials,
                    "languages": params.languages,
                    "coached_organizations": params.coached_organizations,
                    "github_run_url": os.environ.get("GITHUB_RUN_URL"),
                    "triggered_by": os.environ.get("GITHUB_TRIGGERED_BY", "GitHub Actions"),
                },
            )
            print(f"Airtable Scrape Run created: {scrape_run_id}")
        except Exception as exc:
            print(f"WARN: failed to create Airtable Scrape Run: {exc}", file=sys.stderr)
            at_writer = None  # Disable to avoid downstream errors
    else:
        print("Airtable write-back: DISABLED (set AIRTABLE_PAT to enable)")

    started = time.time()
    total_rows = 0
    per_country: dict[str, int] = {}
    airtable_created = 0
    airtable_updated = 0
    airtable_skipped = 0
    scraped_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    headers_to_dict_keys = OUTPUT_HEADERS  # rows align to this order

    with open(params.output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(OUTPUT_HEADERS)

        for country in params.countries:
            browser = init_browser(headless=params.headless)
            try:
                for row in run_country(browser, country, params):
                    while len(row) < len(OUTPUT_HEADERS) - 3:
                        row.append("")
                    row = list(row[: len(OUTPUT_HEADERS) - 3]) + [
                        country.name, params.run_label, scraped_at,
                    ]
                    writer.writerow(row)
                    total_rows += 1
                    per_country[country.name] = per_country.get(country.name, 0) + 1

                    # Airtable write-back, per row, with retry on transient failures
                    if at_writer and scrape_run_id:
                        row_dict = dict(zip(headers_to_dict_keys, row))
                        try:
                            result = at_writer.upsert_coach(row_dict, scrape_run_id)
                            if result == "created":
                                airtable_created += 1
                            elif result == "updated":
                                airtable_updated += 1
                            else:
                                airtable_skipped += 1
                        except Exception as exc:
                            airtable_skipped += 1
                            print(f"WARN: Airtable upsert failed for "
                                  f"{row_dict.get('Email','?')}: {exc}", file=sys.stderr)
            finally:
                browser.quit()

    duration = time.time() - started
    summary = {
        "run_label": params.run_label,
        "total_rows": total_rows,
        "per_country": per_country,
        "duration_seconds": round(duration, 1),
        "output_path": params.output_path,
    }
    if at_writer:
        summary["airtable"] = {
            "scrape_run_id": scrape_run_id,
            "created": airtable_created,
            "updated": airtable_updated,
            "skipped": airtable_skipped,
        }
        try:
            at_writer.finish_scrape_run(scrape_run_id, status="Completed")
        except Exception as exc:
            print(f"WARN: failed to finalise Scrape Run: {exc}", file=sys.stderr)

    print(f"\n=== Done ===")
    print(json.dumps(summary, indent=2))
    return summary


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_params_file(path: str) -> RunParams:
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    countries = [CountryParams(**c) for c in raw.get("countries", [])]
    if not countries:
        sys.exit("ERROR: params.countries must contain at least one entry.")
    return RunParams(
        countries=countries,
        credentials=raw.get("credentials", []),
        languages=raw.get("languages", []),
        coached_organizations=raw.get("coached_organizations", []),
        run_label=raw.get("run_label", "scrape"),
        output_path=raw.get("output_path", f"{raw.get('run_label', 'scrape')}.csv"),
        headless=raw.get("headless", True),
        page_load_wait=raw.get("page_load_wait", 30),
    )


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--params", required=True,
                   help="Path to JSON params file. See params.example.json.")
    args = p.parse_args()

    params = parse_params_file(args.params)
    print(f"Run: {params.run_label}")
    print(f"  countries: {[c.name for c in params.countries]}")
    print(f"  credentials: {params.credentials}")
    print(f"  languages: {params.languages}")
    print(f"  coached_orgs: {params.coached_organizations}")
    print(f"  output: {params.output_path}")
    print(f"  headless: {params.headless}")

    Runner(params)


if __name__ == "__main__":
    main()
