// Airtable Automation script — POSTs to GitHub repository_dispatch when a
// Search Brief record is created. Goes into the "Run script" action of the
// Automation that fires on "When record matches conditions" (Status = "New")
// in the Search Briefs table.
//
// Setup:
//   1. In the Automation builder, add a trigger: "When a record matches
//      conditions" → table = Search Briefs, condition = Status is "New".
//   2. Add an action: "Run a script".
//   3. Paste this whole file in.
//   4. Configure input variables (left sidebar in script editor):
//      - briefId (record id)        → from the trigger record
//      - briefName (text)           → field "Brief Name"
//      - countryNames (text)        → field "Countries" (rollup of Country Name)
//      - credentials (text)         → field "Credentials" (multi-select as csv)
//      - languages (text)           → field "Languages"
//      - clientTypes (text)         → field "Client Types"
//      - submitter (text)           → field "Submitter"
//   5. Replace GITHUB_OWNER, GITHUB_REPO, and GITHUB_PAT below.
//      (Future: move PAT to a secret env var when Airtable supports it.)
//
// After this fires, the GitHub workflow runs and the scraper writes coaches
// directly back into the Coaches table, linked to this brief via Scrape Run.

const config = input.config();

// Helper — Airtable input.config() returns arrays for linked-record and
// multi-select fields, and strings for text fields. Coerce both to a clean
// trimmed string array.
function toArray(v) {
    if (v === null || v === undefined || v === "") return [];
    if (Array.isArray(v)) {
        return v
            .map(x => (typeof x === "object" && x !== null) ? (x.name || x.value || "") : x)
            .map(s => String(s).trim())
            .filter(Boolean);
    }
    return String(v).split(",").map(s => s.trim()).filter(Boolean);
}

// ---- 1. Read the trigger record's fields ----
const briefId      = config.briefId;
const briefName    = config.briefName || `brief_${Date.now()}`;
const countryNames = toArray(config.countryNames);
const credentials  = toArray(config.credentials);
const languages    = toArray(config.languages);
const clientTypes  = toArray(config.clientTypes);
const submitter    = config.submitter || "Airtable Form";

if (countryNames.length === 0) {
    console.log("No countries on the brief — skipping dispatch.");
    return;
}

// ---- 2. Map country names → ICF country codes ----
// Reads the Countries table to get the dialing code for each requested country.
// Saves a manual lookup in the form and keeps codes in one place.
const countriesTable = base.getTable("Countries");
const countryQuery = await countriesTable.selectRecordsAsync({
    fields: ["Country Name", "ICF Code"]
});
const codeByName = new Map();
for (const rec of countryQuery.records) {
    const name = rec.getCellValueAsString("Country Name");
    const code = rec.getCellValueAsString("ICF Code");
    if (name && code) codeByName.set(name.toLowerCase(), code);
}

const countryPayload = countryNames.map(n => ({
    name: n,
    code: codeByName.get(n.toLowerCase()) || ""
}));

// ---- 3. Build the dispatch payload ----
const payload = {
    event_type: "run-scraper",
    client_payload: {
        run_label: briefName,
        brief_id:  briefId,
        submitter: submitter,
        countries: countryPayload,
        credentials: credentials.length ? credentials : ["ACC", "PCC", "MCC"],
        languages: languages,
        coached_organizations: clientTypes
    }
};

// ---- 4. POST to GitHub ----
const GITHUB_OWNER = "REPLACE_WITH_YOUR_GITHUB_USERNAME";
const GITHUB_REPO  = "ICF-_Coaches_Database_Extractor";
const GITHUB_PAT   = "REPLACE_WITH_GITHUB_PAT_REPO_SCOPE";

const response = await fetch(
    `https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/dispatches`,
    {
        method: "POST",
        headers: {
            "Accept": "application/vnd.github+json",
            "Authorization": `Bearer ${GITHUB_PAT}`,
            "X-GitHub-Api-Version": "2022-11-28",
            "Content-Type": "application/json"
        },
        body: JSON.stringify(payload)
    }
);

if (response.status === 204) {
    console.log(`✓ Dispatch sent for brief ${briefName} (${countryNames.length} countries)`);
    // Update the brief's status so we don't re-trigger
    const briefsTable = base.getTable("Search Briefs");
    await briefsTable.updateRecordAsync(briefId, {
        "Status": { name: "Scrape Triggered" }
    });
} else {
    const text = await response.text();
    console.error(`✗ Dispatch failed: ${response.status} ${text}`);
    const briefsTable = base.getTable("Search Briefs");
    await briefsTable.updateRecordAsync(briefId, {
        "Status": { name: "Closed" },
        "Notes": `Dispatch failed: HTTP ${response.status}\n${text.slice(0, 500)}`
    });
}
