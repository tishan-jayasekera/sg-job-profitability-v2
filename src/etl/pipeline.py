import os
import pandas as pd
import numpy as np
from src.etl.cleaners import (
    clean_job_no, clean_task_name, clean_dept, 
    month_key_first_of_month, truthy_excluded, weighted_mode, norm_str
)

# Configuration
RAW_DATA_PATH = "data/raw/Quoted_Task_Report_FY26.xlsx"
OUTPUT_DIR = "data/processed"

def run_pipeline(excel_path=RAW_DATA_PATH, output_dir=OUTPUT_DIR):
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
    ts["cost"] = (ts["hours"] * pd.to_numeric(ts["[Task] Base Rate"], errors="coerce").fillna(0.0)).astype(float)
    ts["Department_clean"] = ts["Department"].map(clean_dept)

    # Aggregate Timesheets
    def agg_timesheet(g):
        total_hours = g["hours"].sum()
        dept_top, dept_share, _ = weighted_mode(g["Department_clean"], g["hours"])
        return pd.Series({
            "total_hours": total_hours,
            "total_cost": g["cost"].sum(),
            "Department_actual": dept_top,
            "distinct_staff": g["[Staff] Name"].nunique()
        })

    timesheet_task_month = ts.groupby(["job_no", "task_name", "month_key"]).apply(agg_timesheet).reset_index()

    # 4. Allocate Revenue
    print("Allocating Revenue...")
    tm = timesheet_task_month.merge(revenue_monthly, how="left", on=["job_no", "month_key"])
    tm["revenue_monthly"] = tm["revenue_monthly"].fillna(0.0)

    # Calculate Totals for Allocation
    job_month_totals = tm.groupby(["job_no", "month_key"], as_index=False).agg(total_hours_job_month=("total_hours", "sum"))
    tm = tm.merge(job_month_totals, on=["job_no", "month_key"], how="left")

    tm["task_share"] = np.where(
        tm["total_hours_job_month"] > 0,
        tm["total_hours"] / tm["total_hours_job_month"],
        0.0
    )
    tm["revenue_allocated"] = tm["task_share"] * tm["revenue_monthly"]

    # Handle Unallocated Revenue
    rev_only = revenue_monthly.merge(job_month_totals, how="left", on=["job_no", "month_key"])
    rev_only["total_hours_job_month"] = rev_only["total_hours_job_month"].fillna(0.0)
    unalloc_rows = rev_only[(rev_only["revenue_monthly"] != 0) & (rev_only["total_hours_job_month"] <= 0)].copy()
    
    if not unalloc_rows.empty:
        unalloc_rows = unalloc_rows.assign(
            task_name="__UNALLOCATED__",
            total_hours=0.0,
            revenue_allocated=lambda x: x["revenue_monthly"],
            total_cost=0.0,
            Department_actual=""
        )
        # Keep only relevant columns
        unalloc_rows = unalloc_rows[tm.columns.intersection(unalloc_rows.columns)]
        fact = pd.concat([tm, unalloc_rows], ignore_index=True)
    else:
        fact = tm.copy()

    # 5. Process Quotes
    print("Processing Quotes...")
    qt["job_no"] = qt["[Job] Job No."].map(clean_job_no)
    qt["task_name"] = qt["[Job Task] Name"].map(clean_task_name)
    qt["quoted_time"] = pd.to_numeric(qt["[Job Task] Quoted Time"], errors="coerce").fillna(0.0)
    qt["quoted_amount"] = pd.to_numeric(qt["[Job Task] Quoted Amount"], errors="coerce").fillna(0.0)
    qt["Department_quote_clean"] = qt["Department"].map(clean_dept)

    def agg_quote(g):
        dept_top, _, _ = weighted_mode(g["Department_quote_clean"], g["quoted_time"].replace(0, 1))
        return pd.Series({
            "quoted_time": g["quoted_time"].sum(),
            "quoted_amount": g["quoted_amount"].sum(),
            "Department_quote": dept_top,
            "Client_quote": str(g["[Job] Client"].iloc[0]) if not g["[Job] Client"].empty else "",
            "Job_Name_quote": str(g["[Job] Name"].iloc[0]) if not g["[Job] Name"].empty else "",
            "Job_Status_quote": str(g["[Job] Status"].iloc[0]) if "[Job] Status" in g else "",
            "Product_quote": str(g["Product"].iloc[0]) if "Product" in g else ""
        })

    quote_task = qt.groupby(["job_no", "task_name"]).apply(agg_quote).reset_index()

    # 6. Create Job-Task Summary (Full Outer Join)
    print("Creating Summaries...")
    actual_task_agg = fact.loc[fact["task_name"] != "__UNALLOCATED__"].groupby(["job_no", "task_name"]).agg({
        "total_hours": "sum",
        "total_cost": "sum",
        "revenue_allocated": "sum",
        "Department_actual": "first" # Simplified for MVP
    }).reset_index().rename(columns={"total_hours": "actual_hours", "total_cost": "actual_cost"})

    job_task_summary = quote_task.merge(actual_task_agg, how="outer", on=["job_no", "task_name"]).fillna(0)
    
    # Flags
    job_task_summary["is_scope_creep"] = (job_task_summary["quoted_time"] == 0) & (job_task_summary["actual_hours"] > 0)
    job_task_summary["Department_reporting"] = np.where(
        job_task_summary["Department_actual"] != 0,
        job_task_summary["Department_actual"],
        job_task_summary["Department_quote"]
    )

    # 7. Export
    os.makedirs(output_dir, exist_ok=True)
    fact.to_csv(os.path.join(output_dir, "fact_job_task_month.csv"), index=False)
    job_task_summary.to_csv(os.path.join(output_dir, "job_task_summary.csv"), index=False)
    
    print(f"ETL Complete. Outputs saved to {output_dir}")

if __name__ == "__main__":
    run_pipeline()