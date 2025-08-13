# Talend Qlik Exports (PowerShell)

Export Talend Cloud catalog and Observability executions to CSV, **combine incrementally** into a single `executions.csv`, remove duplicates fast, and produce derived summaries and metrics.

> **OS**: Windows (uses `sort.exe`).  
> **PowerShell**: 5.1+ (Windows PowerShell) or newer.  
> **Auth**: Talend **PAT** required (`-Pat` or `TALEND_PAT` env var).

---

## Features

- **Catalog**: `artifacts.csv`, `tasks.csv`, `plans.csv`, `connections.csv`, `resources.csv`, plus `plan_steps.csv` and `task_plan.csv`.
- **Executions**: TASKS and PLANS from Observability endpoint (window `[now - DeltaDays, now]`, overlap to avoid gaps).
- **Incremental combine**: append only the delta of `executions_tasks.csv` and `executions_plans.csv` to `executions.csv` (fast, offset-based).
- **De-dup** (optional): exact line-level dedup with `sort.exe /unique`.
- **Derived**: task/plan health, top errors, queue latency, orphan tasks, artifact drift.
- **Metrics**: composition counts, daily/hourly OK/KO (optional), and recurrent KO in last 6 months.

---

## Quick Start

1. **Clone** the repo.
2. **Set your PAT** (do not commit tokens):

   - In CMD files (temporary/CI):  
     `set "TALEND_PAT=REPLACE_WITH_TALEND_PAT"`

   - Or in your shell/profile:  
     `setx TALEND_PAT "YOUR_TOKEN"`

3. **Adjust working paths** in `run_delta.cmd` / `run_full.cmd` (`WORKDIR`).

4. **Run incremental** (recommended for daily/regular usage):

   ```bat
   run_delta.cmd

5. **Run full** (occasionally, with cleanup of state files):

   ```bat
   run_full.cmd

Outputs go to .\talend_exports\.

---

## Script Parameters (highlights)

-Mode: all | catalog | executions | observability | derived | metrics
-Pat: Talend PAT (or env var TALEND_PAT)
-RegionApi: e.g. https://api.eu.cloud.talend.com
-DeltaDays: days back for the fixed execution window (default 1)
-FullRescan: force full export (ignores execution anchors)
-UseSortLineDedup: after combining, run line-level dedup on executions.csv
-DisableExecDailyHourly: skip daily/hourly metrics (speeds up large runs)
-EnvIncludeNames / -EnvIncludeIds: export only those environments
-ExecOverlapMinutes: overlap between windows to avoid lastId gaps (default 15)
-RecurrentKOThreshold: min failures to list a “recurrent KO” group (default 2)
-ExportCopyDir: optional final copy folder (leave empty to skip)

---

## How it works
- Executions are pulled via POST /monitoring/observability/executions/download with:
  category=ETL
  exclude=PLAN_EXECUTIONS for tasks
  exclude=TASK_EXECUTIONS_TRIGGERED_BY_PLAN for plans
  window: [now - DeltaDays, now]
  pagination by lastId within the fixed window
- A per-env, per-kind (tasks|plans) set of seen runIds prevents re-append within a session/file.
- executions.csv is combined incrementally: the script appends only the delta bytes since the last run.
- Optional exact-line dedup (-UseSortLineDedup) uses sort.exe /unique.

Note: it orders the body; if you need strict chronological order, sort downstream.

---

## State & Files

.state/combine_offsets.json — last byte offsets used for combining.
.state/seen_runids_tasks_<env>.txt, .state/seen_runids_plans_<env>.txt — per-kind env seen sets.
executions_tasks.csv / executions_plans.csv — raw per-kind incremental stores.
executions.csv — combined output (de-duplicated if -UseSortLineDedup).

---

## For full rescan you can safely delete:

- talend_exports\executions.csv
- talend_exports\.state\combine_offsets.json
- talend_exports\.state\seen_runids_*.txt

(See run_full.cmd for an example.)

---

##Performance Tips

- Leave -DisableExecDailyHourly on if you don’t need those aggregates frequently.
- Keep -UseSortLineDedup to ensure no duplicates across multiple sessions.
- Increase -MaxExecCsvRowsPerPage (default 100000) if the tenant allows bigger pages.
- Tune -SleepMsBetweenPages if you hit rate limiting; script already retries on 429/5xx/timeouts.

---

## Scheduling

- Use Windows Task Scheduler to run run_delta.cmd periodically.
- Store the PAT as a machine/user environment variable or in a secret manager — avoid committing tokens.

---

## Troubleshooting
- 401/403: invalid PAT or insufficient permissions.
- 429 or 5xx: script retries with exponential backoff; consider longer -SleepMsBetweenPages.
- Duplicates in executions.csv: ensure -UseSortLineDedup is set; remember it sorts the body.
- Empty outputs: verify -EnvIncludeNames/-EnvIncludeIds filters and the selected region.

---

## Security
- Never commit real PATs or tenant-specific secrets.
- Prefer environment variables or your CI/CD secret store.
