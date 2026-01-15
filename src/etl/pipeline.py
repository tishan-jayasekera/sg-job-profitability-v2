import os
import pandas as pd
import numpy as np

from src.etl.cleaners import (
    clean_job_no,
    clean_task_name,
    clean_dept,
    month_key_first_of_month,
    truthy_excluded,
    weighted_mode,
)

# Configuration
RAW_DATA_PATH = "data/raw/Quoted_Task_Report_FY26.xlsx"
OUTPUT_DIR = "data/processed"


def _first_non_null(series: pd.Series):
    non_null = series.dropna()
    return non_null.iloc[0] if not non_null.empty else pd.NaT


def _safe_str(value) -> str:
    return "" if pd.isna(value) else str(value)


def run_pipeline(excel_path: str = RAW_DATA_PATH, output_dir: str = OUTPUT_DIR):
    print(f"Starting ETL Pipeline using {excel_path}...")

    # 1. Load Data
    print("Loading Excel sheets...")
    rev = pd.read_excel(excel_path, sheet_name="Monthly Revenue")
    ts = pd.read_excel(excel_path, sheet_name="Timesheet Data")
    qt = pd.read_excel(excel_path, sheet_name="Quotation Data")

    # 2. Process Revenue (Job-Month)
    print("Processing Revenue...")
    rev["job_no"] = rev["Job Number"].map(clean_job_no)
    rev["month_key"] = month_key_first_of_month(rev["Month"])
    rev["excluded_flag"] = rev["Excluded"].map(truthy_excluded)
    rev["amount"] = pd.to_numeric(rev["Amount"], errors="coerce").fillna(0.0)

    rev_clean = rev[~rev["excluded_flag"]].copy()
    revenue_monthly = (
        rev_clean.groupby(["job_no", "month_key"], as_index=False)
        .agg(revenue_monthly=("amount", "sum"))
    )

    # 3. Process Timesheets (Job-Task-Month)
    print("Processing Timesheets...")
    ts["job_no"] = ts["[Job] Job No."].map(clean_job_no)
    ts["task_name"] = ts["[Job Task] Name"].map(clean_task_name)
    ts["month_key"] = pd.to_datetime(ts["Month Key"], errors="coerce")

    # Fallback for missing Month Keys
    missing_mk = ts["month_key"].isna()
    if missing_mk.any():
        ts.loc[missing_mk, "month_key"] = month_key_first_of_month(ts.loc[missing_mk, "[Time] Date"])

    ts["hours"] = pd.to_numeric(ts["[Time] Time"], errors="coerce").fillna(0.0)
    ts["base_rate"] = pd.to_numeric(ts["[Task] Base Rate"], errors="coerce").fillna(0.0)
    ts["billable_rate"] = pd.to_numeric(ts["[Task] Billable Rate"], errors="coerce").fillna(0.0)
    ts["cost"] = (ts["hours"] * ts["base_rate"]).astype(float)
    ts["billable_value"] = (ts["hours"] * ts["billable_rate"]).astype(float)
    ts["Department_clean"] = ts["Department"].map(clean_dept)

    def agg_timesheet(group: pd.DataFrame) -> pd.Series:
        total_hours = group["hours"].sum()
        dept_top, dept_share, _ = weighted_mode(group["Department_clean"], group["hours"])
        return pd.Series({
            "actual_hours": total_hours,
            "actual_cost": group["cost"].sum(),
            "billable_value": group["billable_value"].sum(),
            "Department_actual": dept_top,
            "department_actual_share": dept_share,
            "distinct_staff": group["[Staff] Name"].nunique(),
        })

    timesheet_task_month = (
        ts.groupby(["job_no", "task_name", "month_key"])
        .apply(agg_timesheet)
        .reset_index()
    )

    # 4. Allocate Revenue
    print("Allocating Revenue...")
    tm = timesheet_task_month.merge(revenue_monthly, how="left", on=["job_no", "month_key"])
    tm["revenue_monthly"] = tm["revenue_monthly"].fillna(0.0)

    job_month_totals = (
        tm.groupby(["job_no", "month_key"], as_index=False)
        .agg(total_hours_job_month=("actual_hours", "sum"))
    )
    tm = tm.merge(job_month_totals, on=["job_no", "month_key"], how="left")

    tm["task_share"] = np.where(
        tm["total_hours_job_month"] > 0,
        tm["actual_hours"] / tm["total_hours_job_month"],
        0.0,
    )
    tm["revenue_allocated"] = tm["task_share"] * tm["revenue_monthly"]
    tm["is_unallocated"] = False
    tm["is_quote_only"] = False

    # Handle Unallocated Revenue
    rev_only = revenue_monthly.merge(job_month_totals, how="left", on=["job_no", "month_key"])
    rev_only["total_hours_job_month"] = rev_only["total_hours_job_month"].fillna(0.0)
    unalloc_rows = rev_only[(rev_only["revenue_monthly"] != 0) & (rev_only["total_hours_job_month"] <= 0)].copy()

    if not unalloc_rows.empty:
        unalloc_rows = unalloc_rows.assign(
            task_name="__UNALLOCATED__",
            actual_hours=0.0,
            actual_cost=0.0,
            billable_value=0.0,
            Department_actual="",
            department_actual_share=0.0,
            distinct_staff=0,
            task_share=0.0,
            revenue_allocated=lambda x: x["revenue_monthly"],
            is_unallocated=True,
            is_quote_only=False,
        )
        unalloc_rows = unalloc_rows[tm.columns.intersection(unalloc_rows.columns)]
        fact_actual = pd.concat([tm, unalloc_rows], ignore_index=True)
    else:
        fact_actual = tm.copy()

    # 5. Process Quotes (Job-Task Budget)
    print("Processing Quotes...")
    qt["job_no"] = qt["[Job] Job No."].map(clean_job_no)
    qt["task_name"] = qt["[Job Task] Name"].map(clean_task_name)
    qt["quoted_time"] = pd.to_numeric(qt["[Job Task] Quoted Time"], errors="coerce").fillna(0.0)
    qt["quoted_amount"] = pd.to_numeric(qt["[Job Task] Quoted Amount"], errors="coerce").fillna(0.0)
    qt["Department_quote_clean"] = qt["Department"].map(clean_dept)

    qt["quote_month_key"] = month_key_first_of_month(
        qt["[Job Task] Start Date"]
        .fillna(qt["[Job] Start Date"])
        .fillna(qt["[Job Task] Due Date"])
    )

    def agg_quote(group: pd.DataFrame) -> pd.Series:
        dept_top, _, _ = weighted_mode(group["Department_quote_clean"], group["quoted_time"].replace(0, 1))
        return pd.Series({
            "quoted_time": group["quoted_time"].sum(),
            "quoted_amount": group["quoted_amount"].sum(),
            "Department_quote": dept_top,
            "Client_quote": _safe_str(group["[Job] Client"].iloc[0]) if not group["[Job] Client"].empty else "",
            "Job_Name_quote": _safe_str(group["[Job] Name"].iloc[0]) if not group["[Job] Name"].empty else "",
            "Job_Status_quote": _safe_str(group["[Job] Status"].iloc[0]) if "[Job] Status" in group else "",
            "Product_quote": _safe_str(group["Product"].iloc[0]) if "Product" in group else "",
            "quote_month_key": _first_non_null(group["quote_month_key"]),
        })

    quote_task = (
        qt.groupby(["job_no", "task_name"])
        .apply(agg_quote)
        .reset_index()
    )

    # 6. Quote-only tasks (no actuals)
    actual_keys = timesheet_task_month[["job_no", "task_name"]].drop_duplicates()
    quote_only = quote_task.merge(actual_keys, how="left", on=["job_no", "task_name"], indicator=True)
    quote_only = quote_only[quote_only["_merge"] == "left_only"].drop(columns=["_merge"])
    quote_only = quote_only[quote_only["quote_month_key"].notna()].copy()

    if not quote_only.empty:
        quote_only = quote_only.assign(
            month_key=quote_only["quote_month_key"],
            actual_hours=0.0,
            actual_cost=0.0,
            billable_value=0.0,
            revenue_monthly=0.0,
            total_hours_job_month=0.0,
            task_share=0.0,
            revenue_allocated=0.0,
            Department_actual="",
            department_actual_share=0.0,
            distinct_staff=0,
            is_unallocated=False,
            is_quote_only=True,
        )
    else:
        quote_only = pd.DataFrame(columns=quote_task.columns.tolist() + [
            "month_key",
            "actual_hours",
            "actual_cost",
            "billable_value",
            "revenue_monthly",
            "total_hours_job_month",
            "task_share",
            "revenue_allocated",
            "Department_actual",
            "department_actual_share",
            "distinct_staff",
            "is_unallocated",
            "is_quote_only",
        ])

    # 7. Build Canonical Fact Table
    fact_actual = fact_actual.merge(quote_task, how="left", on=["job_no", "task_name"])
    fact = pd.concat([fact_actual, quote_only], ignore_index=True, sort=False)

    fact["is_scope_creep"] = (fact["quoted_time"].fillna(0) == 0) & (fact["actual_hours"].fillna(0) > 0)
    fact["department_match"] = (
        fact["Department_actual"].fillna("") != ""
    ) & (
        fact["Department_quote"].fillna("") != ""
    ) & (
        fact["Department_actual"].fillna("") == fact["Department_quote"].fillna("")
    )

    fact["month_key"] = pd.to_datetime(fact["month_key"], errors="coerce")

    # 8. Export
    os.makedirs(output_dir, exist_ok=True)
    fact.to_csv(os.path.join(output_dir, "fact_job_task_month.csv"), index=False)
    print(f"ETL Complete. Canonical fact table saved to {output_dir}")


if __name__ == "__main__":
    run_pipeline()
