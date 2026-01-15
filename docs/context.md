# Job Profitability & Smart Quoting System

## Executive overview
This system produces a reconciled job profitability fact table and an executive dashboard to diagnose margin erosion and improve quoting discipline. It aligns revenue recognition, execution reality, and commercial plan into a single truth model that supports traceable decisions.

## Data model and grain
**Canonical fact table**: `job_no × task_name × month_key` with allocated revenue, actual cost/hours, and quote attributes.

Supporting summaries:
- `job_month_summary`: job-month rollups
- `job_total_summary`: job-level rollups
- `job_driver_summary`: driver tree contributions
- `task_catalog`: smart quote task intelligence
- `job_template_library`: recommended task bundles
- `job_comps_index`: comparable job index

## Join keys + cleaning rules
- `job_no`: trimmed, uppercased string
- `task_name`: trimmed, whitespace collapsed, lowercased (raw stored in `task_name_raw`)
- `month_key`: first-of-month timestamp
- Task and department mappings are applied **only** via `config/task_name_map.csv` and `config/department_map.csv` (no fuzzy logic)

## Revenue allocation methodology
Revenue is provided at job-month grain. Allocation is proportional to task hours within each job-month:

```
revenue_allocated_task = revenue_monthly × (task_hours / total_job_hours)
```

If `total_job_hours = 0` and revenue exists, a synthetic row is created:
- `task_name = "__UNALLOCATED__"`
- `revenue_allocated = revenue_monthly`
- `actual_hours = 0`, `actual_cost = 0`

This exposes recognition timing gaps instead of hiding them.

## Department actual vs quote reconciliation
- `Department_actual` is computed from timesheets using an hours-weighted mode.
- `Department_quote` comes from quotation data and is normalized to the same casing.
- `dept_match_status` flags reconciliation states:
  - `MATCH`, `MISMATCH`, `MISSING_QUOTE_DEPT`, `MISSING_ACTUAL_DEPT`, `QUOTE_ONLY_TASK`, `ACTUAL_ONLY_TASK`

## Margin erosion driver tree
Driver contributions are computed per job to explain GP gaps:
- Quoted task overruns cost
- Unquoted work cost
- Rate mix impact vs baseline (department median rate)
- Non-billable leakage
- Revenue timing anomalies (unallocated revenue)

Outputs are stored in `job_driver_summary.parquet` and support waterfall analysis.

## Smart quote generator methodology
Task intelligence is computed by department, product, and period:
- Task frequency, hours distribution (median/p75/p90)
- Cost and revenue per hour medians
- Overrun rate, unquoted rate, volatility
- Composite risk score

The generator recommends:
- Task list covering ~80% historical job frequency
- Hours policy (median/p75/p90)
- Cost-up pricing guardrails with target margin
- Evidence: comparable historical jobs

## Implementation notes
- All datasets are saved as Parquet for fast local analytics.
- Streamlit uses cached reads and consistent filters across pages.
- QA checks include allocation reconciliation, uniqueness, and missing department coverage.

## Extension roadmap
- Add staff-level traceability and role mix baselines
- Add configurable fiscal year definitions
- Add predictive quote confidence bands
- Add alert scheduling and automated reporting exports
