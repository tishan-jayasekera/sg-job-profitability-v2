import plotly.graph_objects as go
import plotly.express as px
import streamlit as st

from src.app_data import apply_filters, load_data, render_sidebar

st.set_page_config(page_title="Portfolio Drivers", layout="wide")

data = load_data()
filters = render_sidebar(data["fact"])
filtered = apply_filters(data, filters)

job_driver = filtered["job_driver"]
fact = filtered["fact"]

st.title("Portfolio Drivers")

if job_driver.empty:
    st.info("No driver data available for the selected period.")
    st.stop()

portfolio = job_driver[[
    "quoted_overrun_cost",
    "unquoted_work_cost",
    "rate_mix_impact",
    "nonbillable_leakage",
    "revenue_timing_anomaly",
    "actual_gp",
]].sum(numeric_only=True)

baseline_gp = job_driver["baseline_gp"].sum()
actual_gp = portfolio.get("actual_gp", 0.0)

labels = [
    "Baseline GP",
    "Quoted Overruns",
    "Unquoted Work",
    "Rate Mix",
    "Non-billable",
    "Revenue Timing",
    "Actual GP",
]
values = [
    baseline_gp,
    -portfolio.get("quoted_overrun_cost", 0.0),
    -portfolio.get("unquoted_work_cost", 0.0),
    -portfolio.get("rate_mix_impact", 0.0),
    -portfolio.get("nonbillable_leakage", 0.0),
    -portfolio.get("revenue_timing_anomaly", 0.0),
    actual_gp,
]
measure = ["absolute", "relative", "relative", "relative", "relative", "relative", "total"]

fig = go.Figure(
    go.Waterfall(
        name="Portfolio GP",
        orientation="v",
        measure=measure,
        x=labels,
        y=values,
    )
)
fig.update_layout(title="GP Driver Waterfall")
st.plotly_chart(fig, use_container_width=True)

st.subheader("Driver Contribution by Department")
if not fact.empty:
    dept_driver = fact.groupby("Department_reporting", as_index=False).agg(
        actual_gp=("gp", "sum"),
        unquoted_cost=("actual_cost", lambda s: s[fact.loc[s.index, "is_unquoted_task"]].sum()),
        overrun_hours=("hour_overrun", "sum"),
    )
    dept_driver = dept_driver.sort_values("actual_gp", ascending=False).head(10)
    fig_dept = px.bar(dept_driver, x="Department_reporting", y="actual_gp", title="Top Departments by GP")
    st.plotly_chart(fig_dept, use_container_width=True)

st.subheader("Top Loss Drivers (Tasks)")
loss_tasks = (
    fact.groupby("task_name", as_index=False)
    .agg(gp=("gp", "sum"), actual_cost=("actual_cost", "sum"))
    .sort_values("gp", ascending=True)
    .head(10)
)

st.dataframe(loss_tasks, width="stretch")
