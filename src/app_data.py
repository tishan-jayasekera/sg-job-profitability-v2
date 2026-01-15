import os
from datetime import datetime

import pandas as pd
import streamlit as st

from src.io import read_parquet
from src.utils import read_settings


def _ensure_datetime(df: pd.DataFrame, col: str) -> pd.DataFrame:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


@st.cache_data
def load_data():
    base = "data/processed"
    paths = {
        "fact": os.path.join(base, "fact_job_task_month.parquet"),
        "job_month": os.path.join(base, "job_month_summary.parquet"),
        "job_total": os.path.join(base, "job_total_summary.parquet"),
        "job_driver": os.path.join(base, "job_driver_summary.parquet"),
        "task_catalog": os.path.join(base, "task_catalog.parquet"),
        "job_template": os.path.join(base, "job_template_library.parquet"),
        "job_comps": os.path.join(base, "job_comps_index.parquet"),
        "qa": os.path.join(base, "qa_report.json"),
    }

    for key, path in paths.items():
        if key != "qa" and not os.path.exists(path):
            st.error("Data not found. Run `python scripts/build_dataset.py --input data/raw/Quoted_Task_Report_FY26.xlsx --fy FY26` first.")
            st.stop()

    fact = read_parquet(paths["fact"])
    job_month = read_parquet(paths["job_month"])
    job_total = read_parquet(paths["job_total"])
    job_driver = read_parquet(paths["job_driver"])
    task_catalog = read_parquet(paths["task_catalog"])
    job_template = read_parquet(paths["job_template"])
    job_comps = read_parquet(paths["job_comps"])

    fact = _ensure_datetime(fact, "month_key")
    job_month = _ensure_datetime(job_month, "month_key")

    return {
        "fact": fact,
        "job_month": job_month,
        "job_total": job_total,
        "job_driver": job_driver,
        "task_catalog": task_catalog,
        "job_template": job_template,
        "job_comps": job_comps,
    }


def render_sidebar(fact: pd.DataFrame):
    settings = read_settings()
    st.sidebar.title("Filters")

    month_min = fact["month_key"].min()
    month_max = fact["month_key"].max()

    years = sorted({m.year for m in fact["month_key"].dropna()})
    fy_options = [f"FY{str(year)[-2:]}" for year in years]
    default_fy = settings.get("app", {}).get("default_fy", fy_options[-1] if fy_options else "FY26")

    period_mode = st.sidebar.selectbox("Period Mode", ["FY", "Custom Range", "Last N Months"], index=0)

    if period_mode == "FY":
        fy_choice = st.sidebar.selectbox("Financial Year", fy_options, index=fy_options.index(default_fy) if default_fy in fy_options else 0)
        year = int(fy_choice.replace("FY", "")) + 2000
        start = datetime(year, 1, 1)
        end = datetime(year, 12, 31)
    elif period_mode == "Custom Range":
        start, end = st.sidebar.date_input("Month Range", value=(month_min, month_max))
        start = pd.to_datetime(start)
        end = pd.to_datetime(end)
    else:
        n_months = st.sidebar.selectbox("Last N Months", [3, 6, 12], index=1)
        end = month_max
        start = (end - pd.DateOffset(months=n_months - 1)).to_pydatetime()

    depts = ["ALL"] + sorted([d for d in fact["Department_reporting"].dropna().unique() if d != ""])
    products = ["ALL"] + sorted([p for p in fact["Product"].dropna().unique() if p != ""])

    dept_choice = st.sidebar.selectbox("Department", depts)
    product_choice = st.sidebar.selectbox("Product", products)

    include_unallocated = st.sidebar.checkbox(
        "Include unallocated revenue rows",
        value=settings.get("filters", {}).get("include_unallocated_default", True),
    )
    show_mismatches = st.sidebar.checkbox(
        "Show only dept mismatches",
        value=settings.get("filters", {}).get("show_only_dept_mismatch_default", False),
    )
    billable_only = st.sidebar.checkbox(
        "Billable-only",
        value=settings.get("filters", {}).get("billable_only_default", False),
    )
    onshore_only = st.sidebar.checkbox(
        "Onshore-only",
        value=settings.get("filters", {}).get("onshore_only_default", False),
    )

    return {
        "start": pd.to_datetime(start),
        "end": pd.to_datetime(end),
        "dept": dept_choice,
        "product": product_choice,
        "include_unallocated": include_unallocated,
        "show_mismatches": show_mismatches,
        "billable_only": billable_only,
        "onshore_only": onshore_only,
    }


def apply_filters(data: dict, filters: dict):
    fact = data["fact"].copy()
    fact = fact[(fact["month_key"] >= filters["start"]) & (fact["month_key"] <= filters["end"])]

    if filters["dept"] != "ALL":
        fact = fact[fact["Department_reporting"] == filters["dept"]]

    if filters["product"] != "ALL":
        fact = fact[fact["Product"] == filters["product"]]

    if not filters["include_unallocated"]:
        fact = fact[fact["task_name"] != "__UNALLOCATED__"]

    if filters["show_mismatches"]:
        fact = fact[fact["dept_mismatch"] == True]

    if filters["billable_only"]:
        fact = fact[fact["billable_hours"] > 0]

    if filters["onshore_only"]:
        fact = fact[fact["onshore_hours"] > 0]

    job_nos = fact["job_no"].dropna().unique().tolist()

    job_month = data["job_month"].copy()
    job_month = job_month[(job_month["month_key"] >= filters["start"]) & (job_month["month_key"] <= filters["end"])]
    job_month = job_month[job_month["job_no"].isin(job_nos)]

    job_total = data["job_total"].copy()
    job_total = job_total[job_total["job_no"].isin(job_nos)]

    job_driver = data["job_driver"].copy()
    job_driver = job_driver[job_driver["job_no"].isin(job_nos)]

    task_catalog = data["task_catalog"].copy()
    if filters["dept"] != "ALL":
        task_catalog = task_catalog[task_catalog["dept"] == filters["dept"]]
    if filters["product"] != "ALL":
        task_catalog = task_catalog[task_catalog["Product"] == filters["product"]]

    job_template = data["job_template"].copy()
    if filters["dept"] != "ALL":
        job_template = job_template[job_template["dept"] == filters["dept"]]
    if filters["product"] != "ALL":
        job_template = job_template[job_template["Product"] == filters["product"]]

    return {
        "fact": fact,
        "job_month": job_month,
        "job_total": job_total,
        "job_driver": job_driver,
        "task_catalog": task_catalog,
        "job_template": job_template,
        "job_comps": data["job_comps"],
    }
