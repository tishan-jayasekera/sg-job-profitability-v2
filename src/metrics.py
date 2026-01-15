import pandas as pd
import numpy as np

from src.utils import safe_divide


def build_fact_table(allocated_df: pd.DataFrame, quote_task_df: pd.DataFrame) -> pd.DataFrame:
    fact = allocated_df.merge(quote_task_df, on=["job_no", "task_name"], how="left", suffixes=("", "_quote"))

    quote_only = quote_task_df.merge(
        allocated_df[["job_no", "task_name"]].drop_duplicates(),
        on=["job_no", "task_name"],
        how="left",
        indicator=True,
    )
    quote_only = quote_only[quote_only["_merge"] == "left_only"].drop(columns=["_merge"])
    if not quote_only.empty:
        quote_only = quote_only.assign(
            month_key=quote_only["quote_month_key"],
            actual_hours=0.0,
            billable_hours=0.0,
            onshore_hours=0.0,
            actual_cost=0.0,
            avg_base_rate=0.0,
            avg_billable_rate=0.0,
            distinct_staff_count=0,
            revenue_monthly=0.0,
            total_job_hours=0.0,
            task_share=0.0,
            revenue_allocated=0.0,
            is_unallocated_row=False,
            task_name_raw=quote_only["task_name"],
            job_no_raw=quote_only["job_no"],
        )
        fact = pd.concat([fact, quote_only], ignore_index=True, sort=False)

    fact["quoted_time"] = fact["quoted_time"].fillna(0.0)
    fact["quoted_amount"] = fact["quoted_amount"].fillna(0.0)

    fact["rev_alloc"] = fact["revenue_allocated"]
    fact["gp"] = fact["rev_alloc"] - fact["actual_cost"]
    fact["margin"] = np.where(fact["rev_alloc"] > 0, fact["gp"] / fact["rev_alloc"], 0.0)

    fact["hour_overrun"] = np.where(fact["quoted_time"] > 0, fact["actual_hours"] - fact["quoted_time"], 0.0)
    fact["is_unquoted_task"] = (fact["quoted_time"] == 0) & (fact["actual_hours"] > 0)
    fact["is_quote_only_task"] = (fact["quoted_time"] > 0) & (fact["actual_hours"] == 0)

    fact["Department_reporting"] = fact["Department_actual"].where(
        fact["Department_actual"].fillna("") != "",
        fact["Department_quote"],
    )

    fact["dept_match_status"] = "MATCH"
    fact.loc[(fact["Department_quote"].fillna("") == "") & (fact["Department_actual"].fillna("") != ""), "dept_match_status"] = "MISSING_QUOTE_DEPT"
    fact.loc[(fact["Department_quote"].fillna("") != "") & (fact["Department_actual"].fillna("") == ""), "dept_match_status"] = "MISSING_ACTUAL_DEPT"
    fact.loc[
        (fact["Department_quote"].fillna("") != "")
        & (fact["Department_actual"].fillna("") != "")
        & (fact["Department_quote"].fillna("") != fact["Department_actual"].fillna("")),
        "dept_match_status",
    ] = "MISMATCH"
    fact.loc[fact["is_quote_only_task"], "dept_match_status"] = "QUOTE_ONLY_TASK"
    fact.loc[(fact["is_unquoted_task"]) & (fact["Department_actual"].fillna("") != ""), "dept_match_status"] = "ACTUAL_ONLY_TASK"

    fact["dept_mismatch"] = fact["dept_match_status"] == "MISMATCH"
    fact["mixed_department"] = fact.get("Department_actual_mixed", 0).fillna(0).astype(int)
    fact["dept_top_share"] = fact.get("Department_actual_top_share", 0.0).fillna(0.0)

    fact["rev_per_hour"] = np.where(fact["actual_hours"] > 0, fact["rev_alloc"] / fact["actual_hours"], 0.0)
    fact["cost_per_hour"] = np.where(fact["actual_hours"] > 0, fact["actual_cost"] / fact["actual_hours"], 0.0)

    return fact


def build_job_month_summary(fact: pd.DataFrame) -> pd.DataFrame:
    job_month = (
        fact.groupby(["job_no", "month_key"], as_index=False)
        .agg(
            revenue_monthly=("revenue_monthly", "sum"),
            rev_alloc=("rev_alloc", "sum"),
            actual_cost=("actual_cost", "sum"),
            actual_hours=("actual_hours", "sum"),
            billable_hours=("billable_hours", "sum"),
            onshore_hours=("onshore_hours", "sum"),
            unallocated_revenue=("rev_alloc", lambda s: s[fact.loc[s.index, "is_unallocated_row"]].sum()),
            unquoted_hours=("actual_hours", lambda s: s[fact.loc[s.index, "is_unquoted_task"]].sum()),
            dept_mismatch_hours=("actual_hours", lambda s: s[fact.loc[s.index, "dept_mismatch"]].sum()),
        )
    )
    job_month["gp"] = job_month["rev_alloc"] - job_month["actual_cost"]
    job_month["margin"] = np.where(job_month["rev_alloc"] > 0, job_month["gp"] / job_month["rev_alloc"], 0.0)
    job_month["rev_per_hour"] = np.where(job_month["actual_hours"] > 0, job_month["rev_alloc"] / job_month["actual_hours"], 0.0)
    job_month["cost_per_hour"] = np.where(job_month["actual_hours"] > 0, job_month["actual_cost"] / job_month["actual_hours"], 0.0)
    job_month["unquoted_share"] = np.where(job_month["actual_hours"] > 0, job_month["unquoted_hours"] / job_month["actual_hours"], 0.0)
    job_month["dept_mismatch_share"] = np.where(job_month["actual_hours"] > 0, job_month["dept_mismatch_hours"] / job_month["actual_hours"], 0.0)
    job_month["billable_share"] = np.where(job_month["actual_hours"] > 0, job_month["billable_hours"] / job_month["actual_hours"], 0.0)
    job_month["onshore_share"] = np.where(job_month["actual_hours"] > 0, job_month["onshore_hours"] / job_month["actual_hours"], 0.0)

    return job_month


def build_job_total_summary(fact: pd.DataFrame, quote_task_df: pd.DataFrame) -> pd.DataFrame:
    job_total = (
        fact.groupby("job_no", as_index=False)
        .agg(
            rev_alloc=("rev_alloc", "sum"),
            actual_cost=("actual_cost", "sum"),
            actual_hours=("actual_hours", "sum"),
            billable_hours=("billable_hours", "sum"),
            onshore_hours=("onshore_hours", "sum"),
            unallocated_revenue=("rev_alloc", lambda s: s[fact.loc[s.index, "is_unallocated_row"]].sum()),
            unquoted_hours=("actual_hours", lambda s: s[fact.loc[s.index, "is_unquoted_task"]].sum()),
            dept_mismatch_hours=("actual_hours", lambda s: s[fact.loc[s.index, "dept_mismatch"]].sum()),
        )
    )
    job_total["gp"] = job_total["rev_alloc"] - job_total["actual_cost"]
    job_total["margin"] = np.where(job_total["rev_alloc"] > 0, job_total["gp"] / job_total["rev_alloc"], 0.0)
    job_total["rev_per_hour"] = np.where(job_total["actual_hours"] > 0, job_total["rev_alloc"] / job_total["actual_hours"], 0.0)
    job_total["cost_per_hour"] = np.where(job_total["actual_hours"] > 0, job_total["actual_cost"] / job_total["actual_hours"], 0.0)
    job_total["unquoted_share"] = np.where(job_total["actual_hours"] > 0, job_total["unquoted_hours"] / job_total["actual_hours"], 0.0)
    job_total["dept_mismatch_share"] = np.where(job_total["actual_hours"] > 0, job_total["dept_mismatch_hours"] / job_total["actual_hours"], 0.0)
    job_total["billable_share"] = np.where(job_total["actual_hours"] > 0, job_total["billable_hours"] / job_total["actual_hours"], 0.0)
    job_total["onshore_share"] = np.where(job_total["actual_hours"] > 0, job_total["onshore_hours"] / job_total["actual_hours"], 0.0)

    quote_totals = quote_task_df.groupby("job_no", as_index=False).agg(
        quoted_time_total=("quoted_time", "sum"),
        quoted_amount_total=("quoted_amount", "sum"),
        Client=("Client", "first"),
        Job_Name=("Job_Name", "first"),
    )
    job_total = job_total.merge(quote_totals, on="job_no", how="left")
    job_total["quote_attainment_total"] = np.where(
        job_total["quoted_time_total"] > 0,
        job_total["actual_hours"] / job_total["quoted_time_total"],
        0.0,
    )
    return job_total


def build_job_task_summary(fact: pd.DataFrame) -> pd.DataFrame:
    task_summary = (
        fact.groupby(["job_no", "task_name"], as_index=False)
        .agg(
            actual_hours=("actual_hours", "sum"),
            actual_cost=("actual_cost", "sum"),
            rev_alloc=("rev_alloc", "sum"),
            quoted_time=("quoted_time", "max"),
            quoted_amount=("quoted_amount", "max"),
        )
    )
    task_summary["gp"] = task_summary["rev_alloc"] - task_summary["actual_cost"]
    task_summary["margin"] = np.where(task_summary["rev_alloc"] > 0, task_summary["gp"] / task_summary["rev_alloc"], 0.0)
    task_summary["overrun_hours"] = (task_summary["actual_hours"] - task_summary["quoted_time"]).clip(lower=0)
    task_summary["cost_per_hour"] = np.where(task_summary["actual_hours"] > 0, task_summary["actual_cost"] / task_summary["actual_hours"], 0.0)
    task_summary["overrun_cost"] = task_summary["overrun_hours"] * task_summary["cost_per_hour"]
    return task_summary
