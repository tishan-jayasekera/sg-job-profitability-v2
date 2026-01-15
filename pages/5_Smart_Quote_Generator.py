import json
import numpy as np
import pandas as pd
import streamlit as st

from src.app_data import apply_filters, load_data, render_sidebar
from src.utils import read_settings

st.set_page_config(page_title="Smart Quote Generator", layout="wide")

data = load_data()
filters = render_sidebar(data["fact"])
filtered = apply_filters(data, filters)

fact = filtered["fact"]
task_catalog = filtered["task_catalog"]
job_template = filtered["job_template"]
job_comps = filtered["job_comps"]

st.title("Smart Quote Generator")
settings = read_settings()
coverage_target = settings.get("smart_quote", {}).get("coverage_target", 0.8)

if task_catalog.empty:
    st.info("No task intelligence data for the selected filters.")
    st.stop()

dept_options = sorted(task_catalog["dept"].dropna().unique().tolist())
product_options = sorted(task_catalog["Product"].dropna().unique().tolist())

selected_dept = st.selectbox("Department", dept_options)
selected_product = st.selectbox("Product", product_options)
policy = st.selectbox("Policy", ["Aggressive (Median)", "Balanced (Median + Buffer)", "Conservative (P75)"], index=1)
target_margin = st.slider("Target Margin %", min_value=10, max_value=60, value=30, step=1)

catalog = task_catalog[(task_catalog["dept"] == selected_dept) & (task_catalog["Product"] == selected_product)].copy()
if catalog.empty:
    st.warning("No tasks available for this segment.")
    st.stop()

catalog = catalog.sort_values("task_freq_share", ascending=False)
catalog["cum_share"] = catalog["task_freq_share"].cumsum()
recommended = catalog[catalog["cum_share"] <= coverage_target]
if recommended.empty:
    recommended = catalog.head(10)

if policy.startswith("Aggressive"):
    recommended["suggested_hours"] = recommended["hours_per_job_median"]
elif policy.startswith("Balanced"):
    recommended["suggested_hours"] = recommended["hours_per_job_median"] * 1.1
else:
    recommended["suggested_hours"] = recommended["hours_per_job_p75"]

recommended["expected_cost"] = recommended["suggested_hours"] * recommended["cost_per_hour_median"]
recommended["price_guardrail"] = np.where(
    target_margin < 100,
    recommended["expected_cost"] / (1 - target_margin / 100),
    0.0,
)

recommended["risk_flag"] = np.where(recommended["risk_score"] > recommended["risk_score"].median(), "HIGH", "MEDIUM")

st.subheader("Recommended Task List")
show_cols = [
    "task_name",
    "task_freq_share",
    "suggested_hours",
    "cost_per_hour_median",
    "price_guardrail",
    "risk_score",
    "risk_flag",
]

st.dataframe(recommended[show_cols], width="stretch")

st.subheader("Pricing Summary")
summary_cost = recommended["expected_cost"].sum()
summary_price = recommended["price_guardrail"].sum()

c1, c2 = st.columns(2)
c1.metric("Expected Cost", f"${summary_cost:,.0f}")
c2.metric("Guardrail Price", f"${summary_price:,.0f}")

st.subheader("Evidence: Comparable Jobs")
segment_jobs = fact[(fact["Department_reporting"] == selected_dept) & (fact["Product"] == selected_product)]
segment_summary = segment_jobs.groupby("job_no", as_index=False).agg(
    gp=("gp", "sum"),
    rev_alloc=("rev_alloc", "sum"),
    actual_cost=("actual_cost", "sum"),
)
segment_summary["margin"] = np.where(segment_summary["rev_alloc"] > 0, (segment_summary["gp"] / segment_summary["rev_alloc"]) * 100, 0.0)
segment_summary = segment_summary.sort_values("margin", ascending=False).head(10)

st.dataframe(segment_summary, width="stretch")

st.subheader("Export")
export_df = recommended.copy()
export_csv = export_df.to_csv(index=False).encode("utf-8")
export_json = export_df.to_json(orient="records").encode("utf-8")

st.download_button("Download CSV", data=export_csv, file_name="quote_template.csv")
st.download_button("Download JSON", data=export_json, file_name="quote_template.json")
