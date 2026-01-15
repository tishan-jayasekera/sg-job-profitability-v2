import json
import numpy as np
import pandas as pd

from src.utils import parse_period_label, read_settings


def _percentile(series: pd.Series, q: float) -> float:
    if series.empty:
        return 0.0
    return float(np.percentile(series, q))


def build_task_catalog(fact: pd.DataFrame, period_start=None, period_end=None) -> pd.DataFrame:
    settings = read_settings()
    weights = settings.get("smart_quote", {}).get("risk_weights", {})
    coverage_target = settings.get("smart_quote", {}).get("coverage_target", 0.8)

    df = fact.copy()
    df = df[df["task_name"] != "__UNALLOCATED__"]
    if period_start is not None:
        df = df[df["month_key"] >= period_start]
    if period_end is not None:
        df = df[df["month_key"] <= period_end]

    df["dept"] = df["Department_actual"].where(df["Department_actual"].fillna("") != "", df["Department_quote"])
    df["Product"] = df["Product"].fillna("")

    job_task = (
        df.groupby(["job_no", "dept", "Product", "task_name"], as_index=False)
        .agg(
            actual_hours=("actual_hours", "sum"),
            quoted_time=("quoted_time", "max"),
            actual_cost=("actual_cost", "sum"),
            rev_alloc=("rev_alloc", "sum"),
            dept_mismatch=("dept_mismatch", "max"),
        )
    )
    job_task["cost_per_hour"] = np.where(job_task["actual_hours"] > 0, job_task["actual_cost"] / job_task["actual_hours"], 0.0)
    job_task["rev_per_hour"] = np.where(job_task["actual_hours"] > 0, job_task["rev_alloc"] / job_task["actual_hours"], 0.0)
    job_task["overrun_flag"] = job_task["actual_hours"] > job_task["quoted_time"].fillna(0.0)
    job_task["unquoted_flag"] = (job_task["quoted_time"].fillna(0.0) == 0) & (job_task["actual_hours"] > 0)

    total_jobs = job_task.groupby(["dept", "Product"], as_index=False)["job_no"].nunique().rename(columns={"job_no": "job_count"})

    catalog = (
        job_task.groupby(["dept", "Product", "task_name"], as_index=False)
        .agg(
            task_freq_jobs=("job_no", "nunique"),
            hours_per_job_median=("actual_hours", "median"),
            hours_per_job_mean=("actual_hours", "mean"),
            hours_per_job_p75=("actual_hours", lambda s: _percentile(s, 75)),
            hours_per_job_p90=("actual_hours", lambda s: _percentile(s, 90)),
            cost_per_hour_median=("cost_per_hour", "median"),
            rev_per_hour_median=("rev_per_hour", "median"),
            overrun_rate=("overrun_flag", "mean"),
            unquoted_rate=("unquoted_flag", "mean"),
            dept_mismatch_rate=("dept_mismatch", "mean"),
            volatility=("actual_hours", lambda s: float(np.std(s)) / float(np.mean(s)) if np.mean(s) else 0.0),
        )
    )
    catalog = catalog.merge(total_jobs, on=["dept", "Product"], how="left")
    catalog["task_freq_share"] = np.where(catalog["job_count"] > 0, catalog["task_freq_jobs"] / catalog["job_count"], 0.0)

    w_overrun = weights.get("overrun_rate", 0.4)
    w_volatility = weights.get("volatility", 0.4)
    w_unquoted = weights.get("unquoted_rate", 0.2)
    catalog["risk_score"] = (
        catalog["overrun_rate"] * w_overrun
        + catalog["volatility"] * w_volatility
        + catalog["unquoted_rate"] * w_unquoted
    )

    catalog["period_label"] = parse_period_label(period_start, period_end)
    return catalog


def build_job_template_library(fact: pd.DataFrame, period_start=None, period_end=None) -> pd.DataFrame:
    settings = read_settings()
    coverage_target = settings.get("smart_quote", {}).get("coverage_target", 0.8)

    catalog = build_task_catalog(fact, period_start, period_end)
    templates = []

    for (dept, product), group in catalog.groupby(["dept", "Product"]):
        group = group.sort_values("task_freq_share", ascending=False)
        group["cum_share"] = group["task_freq_share"].cumsum()
        recommended = group[group["cum_share"] <= coverage_target]
        if recommended.empty:
            recommended = group.head(5)

        task_list = recommended["task_name"].tolist()

        job_hours = (
            fact[(fact["Department_reporting"].fillna("") == dept) & (fact["Product"].fillna("") == product)]
            .groupby("job_no", as_index=False)
            .agg(total_hours=("actual_hours", "sum"))
        )
        total_hours = job_hours["total_hours"]

        templates.append({
            "dept": dept,
            "Product": product,
            "recommended_tasks": json.dumps(task_list),
            "expected_hours_median": float(total_hours.median()) if not total_hours.empty else 0.0,
            "expected_hours_p75": float(np.percentile(total_hours, 75)) if len(total_hours) else 0.0,
            "expected_hours_p90": float(np.percentile(total_hours, 90)) if len(total_hours) else 0.0,
            "period_label": parse_period_label(period_start, period_end),
        })

    return pd.DataFrame(templates)
