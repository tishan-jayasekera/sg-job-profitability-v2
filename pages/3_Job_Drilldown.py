import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from src.app_data import apply_filters, load_data, render_sidebar

st.set_page_config(page_title="Job Drilldown", layout="wide")

data = load_data()
filters = render_sidebar(data["fact"])
filtered = apply_filters(data, filters)

job_total = filtered["job_total"]
job_month = filtered["job_month"]
job_driver = filtered["job_driver"]
fact = filtered["fact"]

st.title("Job Drilldown")

job_options = job_total.sort_values("rev_alloc", ascending=False)["job_no"].tolist()
selected_job = st.selectbox("Select Job", job_options)

if not selected_job:
    st.stop()

job_month_sel = job_month[job_month["job_no"] == selected_job]
job_driver_sel = job_driver[job_driver["job_no"] == selected_job]
job_fact = fact[fact["job_no"] == selected_job]

st.subheader("Job P&L Trend")
if not job_month_sel.empty:
    fig = px.line(job_month_sel, x="month_key", y=["rev_alloc", "actual_cost", "gp"], markers=True)
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No month data for selected job.")

st.subheader("Driver Waterfall")
if not job_driver_sel.empty:
    row = job_driver_sel.iloc[0]
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
        row.get("baseline_gp", 0.0),
        -row.get("quoted_overrun_cost", 0.0),
        -row.get("unquoted_work_cost", 0.0),
        -row.get("rate_mix_impact", 0.0),
        -row.get("nonbillable_leakage", 0.0),
        -row.get("revenue_timing_anomaly", 0.0),
        row.get("actual_gp", 0.0),
    ]
    measure = ["absolute", "relative", "relative", "relative", "relative", "relative", "total"]
    fig = go.Figure(go.Waterfall(orientation="v", measure=measure, x=labels, y=values))
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No driver data for selected job.")

st.subheader("Task Traceability")
if not job_fact.empty:
    task_table = job_fact[[
        "task_name",
        "actual_hours",
        "actual_cost",
        "rev_alloc",
        "gp",
        "quoted_time",
        "hour_overrun",
        "dept_match_status",
    ]].sort_values("gp", ascending=True)
    st.dataframe(task_table, width="stretch")
else:
    st.info("No task data for selected job.")
