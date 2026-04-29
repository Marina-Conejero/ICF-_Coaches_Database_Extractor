// Airtable Automation script — Brief Link Coaches
//
// Fires when a Scrape Run record's Status flips to Completed / Partial / Failed.
// Walks the Brief linked to that run and:
//   1. Adds the run's coaches to Brief.Coaches Matched (cumulative across runs)
//   2. Computes a multi-country Scrape Summary across ALL Scrape Runs linked
//      to the Brief (so a DACH brief shows DE + AT + CH stats)
//   3. Updates Brief.Status when all the brief's runs are no longer Running
//
// Setup:
//   - Trigger: When a record matches conditions
//     - Table: Scrape Runs
//     - Conditions: Status is one of [Completed, Partial, Failed]
//   - Action: Run a script (this file)
//   - Input variables (left sidebar):
//     - scrapeRunId   →  type "Airtable record ID"  →  source: trigger record

const config = input.config();
const scrapeRunId = config.scrapeRunId;

const scrapeRunsTable = base.getTable("Scrape Runs");
const briefsTable     = base.getTable("Search Briefs");

// 1. Read the Scrape Run that just changed.
const scrapeRun = await scrapeRunsTable.selectRecordAsync(scrapeRunId, {
    fields: ["Run Label", "Search Briefs", "Coaches", "Status", "Error Log"]
});
if (!scrapeRun) {
    console.log(`Scrape Run ${scrapeRunId} not found, exiting.`);
    return;
}

const briefLinks = scrapeRun.getCellValue("Search Briefs") || [];
if (briefLinks.length === 0) {
    console.log(`Run "${scrapeRun.getCellValueAsString("Run Label")}" has no linked brief — skipping.`);
    return;
}

// 2. For each brief, accumulate coaches and rebuild the summary.
for (const briefRef of briefLinks) {
    const brief = await briefsTable.selectRecordAsync(briefRef.id, {
        fields: ["Brief Name", "Coaches Matched", "Scrape Run", "Status"]
    });
    if (!brief) continue;

    // ---- 2a. Accumulate Coaches Matched on the brief ----
    const existingCoachIds = new Set(
        (brief.getCellValue("Coaches Matched") || []).map(c => c.id)
    );
    const newCoachIds = (scrapeRun.getCellValue("Coaches") || []).map(c => c.id);
    for (const cid of newCoachIds) existingCoachIds.add(cid);
    const allCoachIds = Array.from(existingCoachIds).map(id => ({ id }));

    // ---- 2b. Build the multi-country summary ----
    // Read every Scrape Run linked to this brief, in chronological order.
    const allRunRefs = brief.getCellValue("Scrape Run") || [];
    const allRuns = await Promise.all(
        allRunRefs.map(r => scrapeRunsTable.selectRecordAsync(r.id, {
            fields: ["Run Label", "Status", "Coaches", "Started At", "Error Log"]
        }))
    );

    let totalCaptured = 0;
    let anyRunning = false;
    const summaryLines = [];
    summaryLines.push(`📁 ${brief.getCellValueAsString("Brief Name")}`);
    summaryLines.push("");

    // Sort by started time so the summary reads in run order
    allRuns
        .filter(r => r !== null)
        .sort((a, b) => {
            const ta = a.getCellValueAsString("Started At");
            const tb = b.getCellValueAsString("Started At");
            return ta.localeCompare(tb);
        })
        .forEach(r => {
            const label = r.getCellValueAsString("Run Label");
            // Run Label format: "Brief Name__Country" — extract country
            const countryMatch = label.match(/__(.+)$/);
            const country = countryMatch ? countryMatch[1].replace(/_/g, " ") : label;

            const status = r.getCellValueAsString("Status");
            const coachCount = (r.getCellValue("Coaches") || []).length;
            totalCaptured += coachCount;

            let badge;
            if (status === "Completed") badge = "✅";
            else if (status === "Partial") badge = "⚠️";
            else if (status === "Failed") badge = "❌";
            else if (status === "Running") { badge = "⏳"; anyRunning = true; }
            else badge = "•";

            summaryLines.push(`${badge} ${country}: ${coachCount} coaches (${status})`);
        });

    summaryLines.push("");
    summaryLines.push(`Total: ${totalCaptured} coaches across ${allRuns.length} country run(s)`);
    if (anyRunning) {
        summaryLines.push("⏳ Some runs still in progress — this summary will update as they finish.");
    }

    // ---- 2c. Decide brief Status ----
    // If any run is still Running → keep Status as "Scrape Triggered"
    // If all runs are done → set Status to "Results Ready"
    const updateFields = {
        "Coaches Matched": allCoachIds,
        "Scrape Summary": summaryLines.join("\n"),
    };
    if (!anyRunning) {
        updateFields["Status"] = { name: "Results Ready" };
    }

    await briefsTable.updateRecordAsync(briefRef.id, updateFields);

    console.log(
        `✓ Brief ${briefRef.id}: ${allCoachIds.length} coaches linked, ` +
        `summary updated (${anyRunning ? "still running" : "all done"})`
    );
}
