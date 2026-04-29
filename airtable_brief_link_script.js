// Airtable Automation script — links the coaches captured by a Scrape Run
// back to the Search Brief that triggered it, and updates the Brief's
// Status from "Scrape Triggered" → "Results Ready".
//
// Setup:
//   1. Automations → Create automation → name: "Brief Link Coaches"
//   2. Trigger: "When a record matches conditions"
//      - Table: Scrape Runs
//      - Conditions: Status is "Completed" (also include "Partial" if you
//        want briefs to update even when 0 coaches were captured)
//   3. Action: "Run a script"
//   4. Paste this file's contents.
//   5. Configure ONE input variable (left sidebar of script editor):
//      - scrapeRunId  →  type "Airtable record ID"  →  source: the trigger record
//   6. Test on a recent completed Scrape Run record.
//   7. Toggle the automation ON.
//
// What it does:
//   - Reads the Scrape Run's linked Coaches and linked Search Brief
//   - Sets Brief.Coaches Matched = those coaches (overwrites any prior list)
//   - Updates Brief.Status to "Results Ready"
//   - Logs a one-line summary

const config = input.config();
const scrapeRunId = config.scrapeRunId;

const scrapeRunsTable = base.getTable("Scrape Runs");
const briefsTable = base.getTable("Search Briefs");

// Read the Scrape Run record's linked Coaches and linked Search Briefs.
const scrapeRun = await scrapeRunsTable.selectRecordAsync(scrapeRunId, {
    fields: ["Run Label", "Search Briefs", "Coaches"]
});
if (!scrapeRun) {
    console.log(`Scrape Run ${scrapeRunId} not found, exiting.`);
    return;
}

const briefLinks = scrapeRun.getCellValue("Search Briefs") || [];
const coachLinks = scrapeRun.getCellValue("Coaches") || [];

if (briefLinks.length === 0) {
    console.log(`Run "${scrapeRun.getCellValueAsString("Run Label")}" has no linked Search Brief — skipping.`);
    return;
}

if (coachLinks.length === 0) {
    console.log(`Run "${scrapeRun.getCellValueAsString("Run Label")}" captured 0 coaches — updating brief status only.`);
}

const coachIds = coachLinks.map(c => ({ id: c.id }));

// Update each linked Brief (almost always exactly one).
for (const briefRef of briefLinks) {
    await briefsTable.updateRecordAsync(briefRef.id, {
        "Coaches Matched": coachIds,
        "Status": { name: "Results Ready" }
    });
    console.log(
        `✓ Brief ${briefRef.id}: linked ${coachIds.length} coaches, ` +
        `status → Results Ready`
    );
}
