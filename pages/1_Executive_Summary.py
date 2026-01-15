import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

from src.app_data import apply_filters, load_data, render_sidebar

st.set_page_config(page_title="Executive Summary", layout="wide")

data = load_data()
filters = render_sidebar(data["fact"])
filtered = apply_filters(data, filters)

job_month = filtered["job_month"]
job_total = filtered["job_total"]

st.title("Executive Summary")

rev = job_total["rev_alloc"].sum()
cost = job_total["actual_cost"].sum()
gp = rev - cost
margin = (gp / rev * 100) if rev else 0.0
jobs = job_total["job_no"].nunique()

unquoted_hours = filtered["fact"].loc[filtered["fact"]["is_unquoted_task"], "actual_hours"].sum()
all_hours = filtered["fact"]["actual_hours"].sum()
unquoted_share = (unquoted_hours / all_hours * 100) if all_hours else 0.0

unallocated_rev = filtered["fact"].loc[filtered["fact"]["is_unallocated_row"], "rev_alloc"].sum()
unallocated_share = (unallocated_rev / rev * 100) if rev else 0.0

k1, k2, k3, k4, k5, k6 = st.columns(6)
k1.metric("Revenue", f"${rev:,.0f}")
k2.metric("Cost", f"${cost:,.0f}")
k3.metric("GP", f"${gp:,.0f}")
k4.metric("Margin", f"{margin:.1f}%")
k5.metric("Jobs", f"{jobs:,}")
k6.metric("Unquoted Hours", f"{unquoted_share:.1f}%")

st.subheader("Portfolio Trend")
if not job_month.empty:
    trend = job_month.groupby("month_key", as_index=False).agg(
        revenue=("rev_alloc", "sum"),
        cost=("actual_cost", "sum"),
        gp=("gp", "sum"),
    )
    fig = px.line(trend, x="month_key", y=["revenue", "cost", "gp"], markers=True)
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No data available for the selected period.")

st.subheader("Leaderboards")
col_left, col_right = st.columns(2)

with col_left:
    top_gp = job_total.sort_values("gp", ascending=False).head(10)
    st.caption("Top 10 Jobs by GP")
    st.dataframe(top_gp[["job_no", "Job_Name", "Client", "rev_alloc", "gp", "margin"]], width="stretch")

with col_right:
    bottom_margin = job_total.sort_values("margin", ascending=True).head(10)
    st.caption("Bottom 10 Jobs by Margin")
    st.dataframe(bottom_margin[["job_no", "Job_Name", "Client", "rev_alloc", "margin"]], width="stretch")

st.subheader("So What")
insights = []
if margin < 20:
    insights.append("Portfolio margin is below 20% — focus on top GP leak drivers in Portfolio Drivers.")
if unquoted_share > 10:
    insights.append("Unquoted work exceeds 10% of hours — likely scope creep risk.")
if unallocated_share > 5:
    insights.append("Unallocated revenue exceeds 5% — revenue recognition timing mismatch present.")

if not insights:
    insights.append("Portfolio KPIs are within expected ranges; focus on outlier jobs for action.")

st.markdown("\n".join([f"- {item}" for item in insights]))
