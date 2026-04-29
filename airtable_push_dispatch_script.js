// Airtable Automation script — Trigger Workable bulk push
//
// Fires when Caitlin clicks the "Push Marked Coaches Now" button on the
// Push Control record. POSTs a repository_dispatch to GitHub, which kicks
// off the push.yml workflow. The workflow then reads all Marked-for-Push
// coaches from Airtable and pushes them to Workable.
//
// Setup:
//   1. Create a "Push Control" table with a single record
//      - Field 1: "Action Name" (single line text) — set value to e.g. "Push Marked Coaches Now"
//      - Field 2: "Last Triggered At" (date/time) — auto-updated by this automation
//      - Field 3: "Last Result" (single line text) — auto-updated
//   2. Add a Button field on Coaches OR via a "Trigger" automation:
//      Trigger: When a record matches conditions
//        Table: Push Control
//        Conditions: e.g. "Trigger" checkbox is checked
//      Or: Button on the Push Control record that runs an Automation script
//   3. Action: Run a script (this file)
//   4. Replace GITHUB_OWNER / GITHUB_REPO / GITHUB_PAT below
//   5. Configure input variable:
//      - controlRecordId  →  Airtable record ID  →  source: trigger record

const config = input.config();
const controlRecordId = config.controlRecordId;

const GITHUB_OWNER = "Marina-Conejero";
const GITHUB_REPO  = "ICF-_Coaches_Database_Extractor";
const GITHUB_PAT   = "REPLACE_WITH_GITHUB_PAT_REPO_SCOPE";

// 1. Send the dispatch
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
        body: JSON.stringify({
            event_type: "push-coaches",
            client_payload: {
                triggered_by: "Airtable button",
                triggered_at: new Date().toISOString()
            }
        })
    }
);

// 2. Update the Push Control record so Caitlin can see what happened
const controlTable = base.getTable("Push Control");
const isOk = response.status === 204;
await controlTable.updateRecordAsync(controlRecordId, {
    "Last Triggered At": new Date().toISOString(),
    "Last Result": isOk
        ? "✓ Push started — coaches will be processed in the background. Watch Slack for the summary."
        : `✗ Failed to start push: HTTP ${response.status}`
});

if (isOk) {
    console.log("✓ Push workflow triggered on GitHub. Slack will summarise on completion.");
} else {
    const text = await response.text();
    console.error(`✗ Dispatch failed: ${response.status} ${text}`);
}
