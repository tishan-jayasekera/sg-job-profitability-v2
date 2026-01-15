# Job Profitability & Smart Quoting

An executive-grade analytics system that reconciles revenue, delivery, and quotes into a single job profitability fact table and a decision-ready Streamlit dashboard.

## Quickstart

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python scripts/build_dataset.py --input data/raw/Quoted_Task_Report_FY26.xlsx --fy FY26
streamlit run app.py
```

## Repo structure

```
repo/
  README.md
  requirements.txt
  .gitignore
  .env.example
  data/
    raw/Quoted_Task_Report_FY26.xlsx
    processed/
      revenue_monthly.parquet
      timesheet_task_month.parquet
      quote_task.parquet
      fact_job_task_month.parquet
      job_month_summary.parquet
      job_total_summary.parquet
      job_driver_summary.parquet
      task_catalog.parquet
      job_template_library.parquet
      job_comps_index.parquet
      qa_report.json
  config/
    settings.yaml
    task_name_map.csv
    department_map.csv
  docs/context.md
  src/
    __init__.py
    io.py
    clean.py
    revenue.py
    timesheet.py
    quotation.py
    allocation.py
    metrics.py
    drivers.py
    quote_intelligence.py
    comps.py
    qa.py
    build.py
    utils.py
  scripts/
    build_dataset.py
  app.py
  pages/
    1_Executive_Summary.py
    2_Portfolio_Drivers.py
    3_Job_Drilldown.py
    4_Task_Traceability.py
    5_Smart_Quote_Generator.py
    6_Data_QA.py
```

## Data sources
- **Monthly Revenue**: job-month revenue recognition
- **Timesheet Data**: daily job-task execution
- **Quotation Data**: job-task scope and pricing

## Notes
- All joins are deterministic; task/department mapping uses config CSVs only.
- Use `docs/context.md` for methodology and driver tree definitions.
