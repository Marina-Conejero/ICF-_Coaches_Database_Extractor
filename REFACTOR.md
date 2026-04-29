# Refactor — parameterised scraper for Hive Mechanics / Sama

This is the new ICF Coach Finder scraper, replacing the original Streamlit-based UI from this repo. The original Streamlit code (`Main.py`, `CF_scraping.py`) is preserved for reference but no longer used.

## What's new

- **`scraper.py`** — Replaces `Main.py` and `CF_scraping.py`. Accepts a JSON params file via `--params`. Multi-country, headless-by-default, criteria-driven.
- **`airtable_writer.py`** — Streams scraped coaches into the "Coach Recruitment" Airtable base in real time, deduplicating by email, creating a Scrape Run record per run.
- **`.github/workflows/scrape.yml`** — Runs the scraper on GitHub Actions. Triggered by `repository_dispatch` (form-driven), `workflow_dispatch` (manual), or quarterly cron.
- **`params.example.json`** — Documents the input shape.

## Run modes

### Local CSV-only (no Airtable)

For testing or one-off scrapes when you don't need to push to Airtable:

```bash
pip install -r requirements.txt
python3 scraper.py --params my_params.json
```

Output is a CSV at `params.output_path`. Same column order as Konrad's original export.

### Local with Airtable write-back

```bash
export AIRTABLE_PAT='patXXXXX...'
python3 scraper.py --params my_params.json
```

Coaches are written to the Coaches table (deduped by email, linked to a freshly-created Scrape Run record) and ALSO written to CSV as a backup. Workable status fields on existing coach records are preserved — the script never clobbers Workable IDs/stages it didn't put there.

### GitHub Actions

The workflow runs the scraper headless. AIRTABLE_PAT must be set as a repo secret. Three triggers:

- **`repository_dispatch`** — POSTed by Airtable Automation when a new Search Brief is created. Payload becomes the scraper params (event_type: `run-scraper`).
- **`workflow_dispatch`** — manual UI trigger from the Actions tab. Useful for ad-hoc runs.
- **Cron** — quarterly refresh on the 1st of January, April, July, October at 02:00 UTC.

Each country runs as its own matrix job (max 3 concurrent), under the 6-hour cap.

## Input format

`params.json`:

```json
{
  "run_label": "DACH_April_2026",
  "countries": [
    {"name": "Germany", "code": "49"},
    {"name": "Switzerland", "code": "41"}
  ],
  "credentials": ["ACC", "PCC"],
  "languages": ["German", "English"],
  "coached_organizations": ["Global/Multi-national"],
  "output_path": "out/DACH_April_2026.csv",
  "headless": true
}
```

Required: `countries`. Everything else is optional.

- `credentials`: subset of `["ACC","PCC","MCC"]`. If omitted, all three are checked.
- `languages`: must match exact display names in the ICF Fluent Languages modal.
- `coached_organizations`: subset of `["Global/Multi-national","Nonprofit/NGO"]`.
- Other ICF filters (Coaching Themes, Industry Sectors, Held Position, Can Provide, Coaching Methods, Standard Rate) — not yet wired up. Capture happens during scrape; filter post-hoc in Airtable.

## Trigger from Airtable

When a Search Brief record is created, an Airtable Automation runs a script that POSTs to:

```
POST https://api.github.com/repos/<owner>/<repo>/dispatches
Authorization: Bearer <github_pat_with_repo_scope>
Accept: application/vnd.github+json

{
  "event_type": "run-scraper",
  "client_payload": {
    "run_label": "<brief_name>",
    "countries": [...],
    "credentials": [...],
    "languages": [...],
    "coached_organizations": [...],
    "submitter": "<who-submitted>"
  }
}
```

The Automation script template is in `airtable_dispatch_script.js`.

## Airtable schema dependency

Hardcoded base / table IDs in `airtable_writer.py`:
- Base: `appKgfCoWcqDKhcnw`
- Coaches: `tbltOjxlouH6oowRW`
- Scrape Runs: `tbl5RQqn9oUDiGeHX`
- Countries: `tblaaQcQb9D8yWS0c`

If the base is rebuilt, update those constants.

## Differences from Konrad's original code

| Aspect | Original | Refactored |
|---|---|---|
| Entry point | `streamlit run Main.py` (UI) | `python3 scraper.py --params x.json` (CLI) |
| Country input | One at a time via Streamlit text field | List of country objects in JSON |
| Credential filter | Hardcoded ACC+PCC+MCC always | Configurable subset |
| Language filter | Not implemented | Modal-based multi-select via `add-fluent-language` |
| Client-type filter | Not implemented | Inline checkboxes for `coached-global`, `coached-non-profit` |
| Browser init | Module-level (opens Chrome on import) | `init_browser()` function, opens only when needed |
| Headless mode | No (visible Chrome) | Yes (configurable) |
| Output | One CSV per country (`{Country}.csv`) | Single aggregated CSV per run + optional Airtable write |
| Phone normalisation | Hardcoded country-code prepend | Dropped — handled at Airtable-write time |
| Streamlit dependency | Required | Removed |
