import os
from typing import Optional

import pandas as pd

from src.allocation import allocate_revenue
from src.comps import build_job_comps_index
from src.drivers import build_driver_summary
from src.io import read_excel_sheets, write_parquet
from src.metrics import build_fact_table, build_job_month_summary, build_job_task_summary, build_job_total_summary
from src.qa import run_qa
from src.quote_intelligence import build_job_template_library, build_task_catalog
from src.revenue import build_revenue_monthly
from src.timesheet import build_timesheet_task_month
from src.quotation import build_quote_task
from src.utils import ensure_dir, fiscal_year_label, setup_logger, write_json


def _filter_fy(df: pd.DataFrame, month_col: str, fy: Optional[str], fy_col: Optional[str] = None) -> pd.DataFrame:
    if not fy:
        return df
    if fy_col and fy_col in df.columns:
        return df[df[fy_col].astype(str) == fy]
    tmp = df.copy()
    tmp[month_col] = pd.to_datetime(tmp[month_col], errors="coerce")
    return tmp[fiscal_year_label(tmp[month_col]) == fy]


def build_dataset(input_path: str, output_dir: str = "data/processed", fy: Optional[str] = None) -> None:
    logger = setup_logger()
    ensure_dir(output_dir)

    logger.info("Loading Excel sheets")
    sheets = read_excel_sheets(input_path)

    revenue = build_revenue_monthly(sheets["revenue"])
    timesheet = build_timesheet_task_month(sheets["timesheet"])
    quote_task = build_quote_task(sheets["quote"])

    if fy:
        revenue = _filter_fy(revenue, "month_key", fy, fy_col="FY") if "FY" in sheets["revenue"].columns else _filter_fy(revenue, "month_key", fy)
        timesheet = _filter_fy(timesheet, "month_key", fy)
        if "quote_month_key" in quote_task.columns:
            quote_task = _filter_fy(quote_task, "quote_month_key", fy)

    write_parquet(revenue, os.path.join(output_dir, "revenue_monthly.parquet"))
    write_parquet(timesheet, os.path.join(output_dir, "timesheet_task_month.parquet"))
    write_parquet(quote_task, os.path.join(output_dir, "quote_task.parquet"))

    logger.info("Allocating revenue")
    allocated = allocate_revenue(timesheet, revenue)

    logger.info("Building fact table")
    fact = build_fact_table(allocated, quote_task)
    job_month = build_job_month_summary(fact)
    job_total = build_job_total_summary(fact, quote_task)
    job_task = build_job_task_summary(fact)

    logger.info("Building drivers and smart quote intelligence")
    driver_summary = build_driver_summary(fact)
    task_catalog = build_task_catalog(fact)
    template_library = build_job_template_library(fact)
    comps_index = build_job_comps_index(fact)

    write_parquet(fact, os.path.join(output_dir, "fact_job_task_month.parquet"))
    write_parquet(job_month, os.path.join(output_dir, "job_month_summary.parquet"))
    write_parquet(job_total, os.path.join(output_dir, "job_total_summary.parquet"))
    write_parquet(job_task, os.path.join(output_dir, "job_task_summary.parquet"))
    write_parquet(driver_summary, os.path.join(output_dir, "job_driver_summary.parquet"))
    write_parquet(task_catalog, os.path.join(output_dir, "task_catalog.parquet"))
    write_parquet(template_library, os.path.join(output_dir, "job_template_library.parquet"))
    write_parquet(comps_index, os.path.join(output_dir, "job_comps_index.parquet"))

    qa_report = run_qa(fact)
    write_json(qa_report, os.path.join(output_dir, "qa_report.json"))

    logger.info("Build complete")
