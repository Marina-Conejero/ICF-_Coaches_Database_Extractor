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
from selenium.common.exceptions import NoSuchElementException
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
    # Note: ACTC is NOT a search-time filter on the ICF directory. It only
    # appears on individual coach profiles. When ACTC is requested as a brief
    # filter, the scraper still runs against ACC/PCC/MCC and we filter for
    # ACTC presence post-scrape (in airtable_writer.link_brief_matches).
}

COACHED_ORG_IDS = {
    "Global/Multi-national": "coached-global",
    "Nonprofit/NGO": "coached-non-profit",
}

# Inline Semantic-UI dropdowns (different DOM pattern from the language /
# location modals). Captured from the live ICF directory May 2026.
TYPE_OF_CLIENT_DROPDOWN_ID = "type-of-client-dropdown"
TYPE_OF_CLIENT_VALUES = {
    # Form-friendly name → data-value used by Semantic UI's dropdown menu
    "Organizational": "Organization",
    "Personal": "Individual",
}

INDUSTRY_SECTOR_DROPDOWN_ID = "industry-sector-dropdown"
INDUSTRY_SECTOR_VALUES = {
    "Communications, Entertainment, and Media":   "aa838cbf-1d56-40a1-8563-047c05f7105f",
    "Education":                                  "a6c708de-34ab-4622-988f-895aff81cc97",
    "Energy and Utilities":                       "c427d281-4c16-4730-9ffc-5f2ca556dcec",
    "Government and Public":                      "d0c88526-147e-4555-8cef-176a4361eea7",
    "Health, Pharmaceutical and Science":         "463183c4-7280-4674-b6af-30b3f1bb58ca",
    "Hospitality and Leisure":                    "c5f322d2-1008-4dc6-aa0d-c18b7db6b1ac",
    "Manufacturing, Engineering and Construction": "f45f7162-9f49-4b68-9b97-881702f46fba",
    "Professional and Financial Services":        "1b6c2344-7256-464b-96f9-4b428841e60d",
    "Retail and Consumer":                        "fbb8838a-89d4-4b2c-83a7-bed1e43d5d9c",
    "Technology":                                 "4c683b7e-178c-4859-9d7b-ebf5dcebebca",
    "Transportation":                             "3042d0e3-389e-488f-886f-51484a1dde79",
}

# Gender dropdown has no DOM id, only a placeholder. We find it by its
# `.default.text` "Gender". Selection values match the visible text.
GENDER_DROPDOWN_PLACEHOLDER = "Gender"
GENDER_VALUES = {"Male": "Male", "Female": "Female"}

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
    "ICF Profile URL",
    # Run metadata appended by Runner:
    "Country", "Run_Label", "Scraped_At",
]

ICF_PROFILE_URL_TEMPLATE = (
    "https://apps.coachingfederation.org/eweb/CCFDynamicPage.aspx"
    "?webcode=ccfcoachprofileview&coachcstkey={key}"
)


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


def apply_inline_dropdown_by_id(browser: webdriver.Chrome,
                                dropdown_id: str,
                                value: str,
                                label_for_logs: str = "") -> None:
    """Select an item in a Semantic UI dropdown identified by `dropdown_id`.

    These are inline `<div class="ui dropdown">` widgets (Type of Client,
    Industry Sectors, Sort, etc) — different from the language/location modals.
    Selection pattern: click the wrapper to open, click the matching menu
    item by `data-value`, dropdown auto-closes.
    """
    if not value:
        return
    label = label_for_logs or dropdown_id
    try:
        script = (
            "const d = document.getElementById(arguments[0]);"
            "if (!d) return 'no-dropdown';"
            "d.click();"
            "const item = d.querySelector(`.menu .item[data-value=\"${arguments[1]}\"]`);"
            "if (!item) return 'no-item';"
            "item.click();"
            "return 'ok';"
        )
        result = browser.execute_script(script, dropdown_id, value)
        if result == "ok":
            print(f"  ✓ {label}: {value}")
        elif result == "no-dropdown":
            print(f"  warning: dropdown #{dropdown_id} not found", file=sys.stderr)
        else:
            print(f"  warning: {label} option {value!r} not found in dropdown", file=sys.stderr)
        time.sleep(0.6)
    except Exception as exc:
        print(f"  warning: could not set {label}={value!r}: {exc}", file=sys.stderr)


def apply_inline_dropdown_by_placeholder(browser: webdriver.Chrome,
                                         placeholder: str,
                                         value: str,
                                         label_for_logs: str = "") -> None:
    """Select an item in a Semantic UI dropdown identified by its placeholder text.

    Used for dropdowns without a stable DOM id — e.g. the Gender dropdown,
    which only exposes `.default.text` = "Gender".
    """
    if not value:
        return
    label = label_for_logs or placeholder
    try:
        script = (
            "const dropdowns = document.querySelectorAll('.ui.dropdown');"
            "const d = Array.from(dropdowns).find(x => "
            "  (x.querySelector('.default.text')?.innerText || '').trim() === arguments[0]"
            ");"
            "if (!d) return 'no-dropdown';"
            "d.click();"
            "const item = d.querySelector(`.menu .item[data-value=\"${arguments[1]}\"]`);"
            "if (!item) return 'no-item';"
            "item.click();"
            "return 'ok';"
        )
        result = browser.execute_script(script, placeholder, value)
        if result == "ok":
            print(f"  ✓ {label}: {value}")
        elif result == "no-dropdown":
            print(f"  warning: dropdown with placeholder {placeholder!r} not found", file=sys.stderr)
        else:
            print(f"  warning: {label} option {value!r} not found", file=sys.stderr)
        time.sleep(0.6)
    except Exception as exc:
        print(f"  warning: could not set {label}={value!r}: {exc}", file=sys.stderr)


def apply_type_of_client_filter(browser: webdriver.Chrome,
                                type_of_client: str) -> None:
    """Set the Type of Client dropdown (Organizational/Personal).

    'Both', 'Any', or blank → no filter applied (returns everyone). ICF's
    dropdown doesn't have a 'Both' option natively; leaving the dropdown
    unset has the same effect.
    """
    if not type_of_client or type_of_client.lower() in {"both", "any", "all", "either", "unspecified"}:
        return
    mapped = TYPE_OF_CLIENT_VALUES.get(type_of_client)
    if not mapped:
        print(f"  warning: unknown Type of Client {type_of_client!r}", file=sys.stderr)
        return
    apply_inline_dropdown_by_id(browser, TYPE_OF_CLIENT_DROPDOWN_ID, mapped,
                                "type-of-client")


def apply_industry_sectors_filter(browser: webdriver.Chrome,
                                  sectors: list[str]) -> None:
    """Pick one or more industry sectors from the Industry Sectors dropdown.

    The dropdown is a Semantic UI single-select that can be re-opened to add
    additional sectors (each click adds the value as a chip). We loop through
    every requested sector, opening the dropdown each time.
    """
    for sector in sectors or []:
        guid = INDUSTRY_SECTOR_VALUES.get(sector)
        if not guid:
            print(f"  warning: unknown Industry Sector {sector!r}", file=sys.stderr)
            continue
        apply_inline_dropdown_by_id(browser, INDUSTRY_SECTOR_DROPDOWN_ID, guid,
                                    f"industry: {sector}")


def apply_gender_filter(browser: webdriver.Chrome, gender: str) -> None:
    """Set the Gender dropdown (Male/Female). 'Any' is a no-op."""
    if not gender or gender.lower() in {"any", "all", "either", "unspecified"}:
        return
    mapped = GENDER_VALUES.get(gender.capitalize())
    if not mapped:
        print(f"  warning: unknown Gender {gender!r}", file=sys.stderr)
        return
    apply_inline_dropdown_by_placeholder(browser, GENDER_DROPDOWN_PLACEHOLDER,
                                         mapped, "gender")


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
    # Append the ICF profile URL as a stable click-through link
    row.append(ICF_PROFILE_URL_TEMPLATE.format(key=coach_value))

    browser.close()
    browser.switch_to.window(browser.window_handles[-1])
    return row


def count_total_cards_on_page(browser: webdriver.Chrome) -> int:
    """How many coach cards are present in the DOM right now."""
    return len(browser.find_elements(By.XPATH, f"//div[@class='{CARD_CLASS}']"))


def iterate_cards(browser: webdriver.Chrome,
                  wait: WebDriverWait,
                  page_num: int) -> tuple[list[list[str]], int, list[str]]:
    """Walk every coach card on the current page, collect rows.

    Returns: (rows, total_cards_on_page, errors).
    Errors is a list of human-readable strings of cards that failed to
    extract — can be empty. Caller uses len(rows) vs total_cards to detect
    silent failures.
    """
    rows: list[list[str]] = []
    errors: list[str] = []
    total = count_total_cards_on_page(browser)
    print(f"  page {page_num}: {total} cards visible")

    for step in range(1, total + 1):
        try:
            card = browser.find_element(
                By.XPATH,
                f"//div[@class='{CARD_CLASS}'][position()={step}]"
            )
            check_input = card.find_element(By.TAG_NAME, "input")
            coach_value = check_input.get_attribute("value")
            row = extract_profile_row(browser, wait, coach_value)
            rows.append(row)
            wait.until(EC.presence_of_element_located((By.ID, CARDS_CONTAINER_ID)))
        except NoSuchElementException as exc:
            msg = f"page {page_num}, card {step}/{total}: element not found"
            errors.append(msg)
            print(f"    ⚠ {msg}", file=sys.stderr)
        except Exception as exc:
            msg = f"page {page_num}, card {step}/{total}: {type(exc).__name__}: {str(exc)[:150]}"
            errors.append(msg)
            print(f"    ⚠ {msg}", file=sys.stderr)

    print(f"  page {page_num}: captured {len(rows)}/{total} coaches"
          + (" (with errors)" if errors else ""))
    return rows, total, errors


def discover_total_pages(browser: webdriver.Chrome) -> int:
    """Read the pagination element to find the maximum page number."""
    try:
        links = browser.find_elements(By.CSS_SELECTOR, "a.item[data-value]")
        page_nums = []
        for link in links:
            v = link.get_attribute("data-value")
            if v and v.isdigit():
                page_nums.append(int(v))
        return max(page_nums) if page_nums else 1
    except Exception:
        return 1


def iterate_pages(browser: webdriver.Chrome,
                  wait: WebDriverWait
                  ) -> tuple[list[list[str]], dict]:
    """Walk every result page, returning (all_rows, diagnostics).

    diagnostics = {
        "pages_seen": int,
        "expected_total_cards": int,    # sum across all pages
        "captured_total": int,
        "errors": list[str],
    }
    """
    all_rows: list[list[str]] = []
    diagnostics = {
        "pages_seen": 0,
        "expected_total_cards": 0,
        "captured_total": 0,
        "errors": [],
    }

    expected_pages = discover_total_pages(browser)
    print(f"\n  pagination: {expected_pages} page(s) detected")

    for page in range(1, expected_pages + 1):
        if page > 1:
            try:
                next_link = browser.find_element(
                    By.XPATH,
                    f"//a[@class='item'][@data-value={page}]"
                )
                if "disabled" in (next_link.get_attribute("class") or ""):
                    diagnostics["errors"].append(
                        f"page {page}: next-page link disabled, stopping"
                    )
                    break
                next_link.click()
                time.sleep(2)
                wait.until(EC.presence_of_element_located((By.ID, CARDS_CONTAINER_ID)))
            except NoSuchElementException:
                diagnostics["errors"].append(
                    f"page {page}: navigation link not found"
                )
                break
            except Exception as exc:
                diagnostics["errors"].append(
                    f"page {page}: navigation error: {type(exc).__name__}: {exc}"
                )
                break

        page_rows, page_total, page_errors = iterate_cards(browser, wait, page)
        diagnostics["pages_seen"] += 1
        diagnostics["expected_total_cards"] += page_total
        diagnostics["captured_total"] += len(page_rows)
        diagnostics["errors"].extend(page_errors)
        all_rows.extend(page_rows)

        if page_total == 0:
            # No more cards — done
            break

    return all_rows, diagnostics


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
    credentials: list[str] = field(default_factory=list)        # ['ACC','PCC','ACTC']
    languages: list[str] = field(default_factory=list)          # ['English','German']
    coached_organizations: list[str] = field(default_factory=list)  # ['Global/Multi-national']
    type_of_client: str = ""                # 'Organizational' or 'Personal'
    industry_sectors: list[str] = field(default_factory=list)   # ['Professional and Financial Services', ...]
    gender: str = ""                        # 'Male', 'Female', or '' (Any)
    run_label: str = "scrape"
    output_path: str = "raw_data.csv"
    headless: bool = True
    page_load_wait: int = 30
    brief_id: str = ""    # Search Brief record id — set when scrape was triggered by a brief


def run_country(browser: webdriver.Chrome,
                country: CountryParams,
                params: RunParams) -> tuple[list[list[str]], dict]:
    """Run a single-country scrape. Returns (rows, diagnostics)."""
    print(f"\n--- {country.name} ---")
    browser.get(ICF_SEARCH_URL)
    wait = WebDriverWait(browser, params.page_load_wait)
    wait.until(EC.presence_of_element_located((By.ID, "credential-acc")))
    wait.until(EC.presence_of_element_located((By.ID, "add-location")))

    # ACTC isn't a search-time filter — drop it from the credentials list
    # we send to the ICF UI; post-scrape ACTC filtering happens in the
    # airtable_writer Brief-linking step.
    scrape_creds = [c for c in (params.credentials or []) if c.upper() != "ACTC"]

    apply_credential_filters(browser, scrape_creds)
    apply_coached_org_filters(browser, params.coached_organizations)
    apply_type_of_client_filter(browser, params.type_of_client)
    apply_industry_sectors_filter(browser, params.industry_sectors)
    apply_gender_filter(browser, params.gender)
    apply_modal_filter(browser, "location", [country.name])
    apply_modal_filter(browser, "language", params.languages)

    # Wait for results.
    try:
        wait.until(EC.presence_of_element_located((By.ID, CARDS_CONTAINER_ID)))
    except Exception:
        print(f"  no results for {country.name}")
        return [], {
            "pages_seen": 0, "expected_total_cards": 0,
            "captured_total": 0, "errors": ["no results card container appeared"],
        }

    return iterate_pages(browser, wait)


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
                    "type_of_client": params.type_of_client,
                    "industry_sectors": params.industry_sectors,
                    "gender": params.gender,
                    "github_run_url": os.environ.get("GITHUB_RUN_URL"),
                    "triggered_by": os.environ.get("GITHUB_TRIGGERED_BY", "GitHub Actions"),
                    "brief_id": params.brief_id or None,
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
    fatal_error: Exception | None = None
    aggregate_diagnostics: list[dict] = []  # one per country

    headers_to_dict_keys = OUTPUT_HEADERS  # rows align to this order

    try:
        with open(params.output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(OUTPUT_HEADERS)

            for country in params.countries:
                browser = init_browser(headless=params.headless)
                try:
                    rows, diag = run_country(browser, country, params)
                    diag["country"] = country.name
                    aggregate_diagnostics.append(diag)
                    for row in rows:
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
                                result = at_writer.upsert_coach(
                                    row_dict, scrape_run_id,
                                    applied_credentials=params.credentials,
                                )
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
    except Exception as exc:
        # Capture the failure but let the finally block run so we mark
        # the Scrape Run as Failed and log details.
        fatal_error = exc
        print(f"\n*** Scraper aborted: {exc}", file=sys.stderr)

    duration = time.time() - started

    # Sanity check — did we capture every coach the page advertised?
    expected_total = sum(d.get("expected_total_cards", 0) for d in aggregate_diagnostics)
    captured_total = sum(d.get("captured_total", 0) for d in aggregate_diagnostics)
    all_errors: list[str] = []
    for d in aggregate_diagnostics:
        country_name = d.get("country", "?")
        for e in d.get("errors", []):
            all_errors.append(f"[{country_name}] {e}")

    is_partial = (
        (expected_total > 0 and captured_total < expected_total)
        or len(all_errors) > 0
        or total_rows == 0
    )

    summary = {
        "run_label": params.run_label,
        "total_rows": total_rows,
        "per_country": per_country,
        "duration_seconds": round(duration, 1),
        "output_path": params.output_path,
        "fatal_error": str(fatal_error) if fatal_error else None,
        "expected_total_cards": expected_total,
        "captured_total": captured_total,
        "error_count": len(all_errors),
    }

    if at_writer:
        summary["airtable"] = {
            "scrape_run_id": scrape_run_id,
            "created": airtable_created,
            "updated": airtable_updated,
            "skipped": airtable_skipped,
        }
        try:
            if fatal_error:
                at_writer.finish_scrape_run(
                    scrape_run_id,
                    status="Failed",
                    error_log=(
                        f"Scraper aborted after {total_rows} rows: {fatal_error}\n\n"
                        + "Diagnostics:\n" + "\n".join(all_errors[:50])
                    ),
                )
            elif is_partial:
                # Compact log for the SINGLE country this Scrape Run covers.
                # Brief-level multi-country aggregation lives on the Brief.
                missed = expected_total - captured_total
                lines: list[str] = []
                if expected_total == 0 and captured_total == 0:
                    lines.append("⚠️ No coaches matched these filters.")
                    lines.append("The combination may be too narrow — try removing one filter and re-run.")
                elif missed > 0:
                    lines.append(f"📊 Captured {captured_total} of {expected_total} coaches.")
                    lines.append(f"⚠️ {missed} missed (ICF page timeouts) — re-run the brief to retry.")
                else:
                    lines.append(f"✅ Captured {captured_total} coaches with {len(all_errors)} minor warnings.")
                if all_errors:
                    lines.append("")
                    lines.append(f"Technical detail ({len(all_errors)} error(s)):")
                    lines.append(f"  • {all_errors[0]}")
                    if len(all_errors) > 1:
                        lines.append(f"  • …and {len(all_errors) - 1} more")
                at_writer.finish_scrape_run(
                    scrape_run_id,
                    status="Partial",
                    error_log="\n".join(lines),
                )
            else:
                at_writer.finish_scrape_run(scrape_run_id, status="Completed")
        except Exception as exc:
            print(f"WARN: failed to finalise Scrape Run: {exc}", file=sys.stderr)

    print(f"\n=== Done ===")
    print(json.dumps(summary, indent=2))

    # Re-raise to keep CI step status correct
    if fatal_error:
        raise fatal_error
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
        type_of_client=raw.get("type_of_client", ""),
        industry_sectors=raw.get("industry_sectors", []),
        gender=raw.get("gender", ""),
        run_label=raw.get("run_label", "scrape"),
        output_path=raw.get("output_path", f"{raw.get('run_label', 'scrape')}.csv"),
        headless=raw.get("headless", True),
        page_load_wait=raw.get("page_load_wait", 30),
        brief_id=raw.get("brief_id", ""),
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
