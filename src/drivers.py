import pandas as pd
import numpy as np


def build_driver_summary(fact: pd.DataFrame) -> pd.DataFrame:
    df = fact.copy()
    df = df[df["task_name"] != "__UNALLOCATED__"]
    df["cost_per_hour"] = np.where(df["actual_hours"] > 0, df["actual_cost"] / df["actual_hours"], 0.0)
    df["dept_for_rate"] = df["Department_actual"].where(df["Department_actual"].fillna("") != "", df["Department_quote"])

    dept_baseline = (
        df[df["actual_hours"] > 0]
        .groupby("dept_for_rate", as_index=False)
        .agg(baseline_rate=("cost_per_hour", "median"))
    )
    df = df.merge(dept_baseline, on="dept_for_rate", how="left")
    df["baseline_rate"] = df["baseline_rate"].fillna(df["cost_per_hour"].median())

    df["overrun_hours"] = (df["actual_hours"] - df["quoted_time"]).clip(lower=0)
    df["quoted_overrun_cost"] = df["overrun_hours"] * df["cost_per_hour"]
    df["unquoted_work_cost"] = np.where(df["is_unquoted_task"], df["actual_cost"], 0.0)
    df["rate_mix_impact"] = df["actual_hours"] * (df["cost_per_hour"] - df["baseline_rate"])
    df["nonbillable_leakage"] = np.where(
        df["actual_hours"] > 0,
        df["actual_cost"] * (1 - (df["billable_hours"] / df["actual_hours"])),
        0.0,
    )

    job_driver = (
        df.groupby("job_no", as_index=False)
        .agg(
            rev_alloc=("rev_alloc", "sum"),
            actual_cost=("actual_cost", "sum"),
            actual_hours=("actual_hours", "sum"),
            quoted_overrun_cost=("quoted_overrun_cost", "sum"),
            unquoted_work_cost=("unquoted_work_cost", "sum"),
            rate_mix_impact=("rate_mix_impact", "sum"),
            nonbillable_leakage=("nonbillable_leakage", "sum"),
            Client=("Client", "first"),
            Job_Name=("Job_Name", "first"),
        )
    )

    unallocated = fact[fact["is_unallocated_row"]].groupby("job_no", as_index=False).agg(
        revenue_timing_anomaly=("rev_alloc", "sum")
    )
    job_driver = job_driver.merge(unallocated, on="job_no", how="left")
    job_driver["revenue_timing_anomaly"] = job_driver["revenue_timing_anomaly"].fillna(0.0)

    baseline_cost = (
        df.groupby("job_no", as_index=False)
        .agg(baseline_cost=("actual_hours", lambda s: (s * df.loc[s.index, "baseline_rate"]).sum()))
    )
    job_driver = job_driver.merge(baseline_cost, on="job_no", how="left")

    job_driver["actual_gp"] = job_driver["rev_alloc"] - job_driver["actual_cost"]
    job_driver["baseline_gp"] = job_driver["rev_alloc"] - job_driver["baseline_cost"]
    job_driver["gp_gap"] = job_driver["baseline_gp"] - job_driver["actual_gp"]
    job_driver["explained_gap"] = (
        job_driver["quoted_overrun_cost"]
        + job_driver["unquoted_work_cost"]
        + job_driver["rate_mix_impact"]
        + job_driver["nonbillable_leakage"]
    )
    job_driver["unexplained_gap"] = job_driver["gp_gap"] - job_driver["explained_gap"]

    return job_driver
