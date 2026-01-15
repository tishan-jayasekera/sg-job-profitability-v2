import streamlit as st

from src.app_data import apply_filters, load_data, render_sidebar

st.set_page_config(page_title="Job Profitability & Smart Quoting", layout="wide")

data = load_data()
filters = render_sidebar(data["fact"])
filtered = apply_filters(data, filters)

st.title("Job Profitability & Smart Quoting")

fact = filtered["fact"]
job_total = filtered["job_total"]

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Revenue", f"${job_total['rev_alloc'].sum():,.0f}")
col2.metric("Cost", f"${job_total['actual_cost'].sum():,.0f}")
col3.metric("GP", f"${(job_total['rev_alloc'].sum() - job_total['actual_cost'].sum()):,.0f}")
col4.metric("Margin", f"{(job_total['rev_alloc'].sum() - job_total['actual_cost'].sum()) / job_total['rev_alloc'].sum() * 100 if job_total['rev_alloc'].sum() else 0:.1f}%")
col5.metric("Jobs", f"{job_total['job_no'].nunique():,}")

st.markdown(
    """
    **How to use this app**
    - Use the sidebar filters to set the period, department, and product.
    - Navigate pages for executive summary, portfolio drivers, job drilldown, task traceability, smart quoting, and data QA.
    """
)

st.subheader("Quick Health Check")

unquoted_hours = fact.loc[fact["is_unquoted_task"], "actual_hours"].sum()
all_hours = fact["actual_hours"].sum()
unquoted_share = (unquoted_hours / all_hours * 100) if all_hours else 0.0

unallocated_rev = fact.loc[fact["is_unallocated_row"], "rev_alloc"].sum()
rev_total = fact["rev_alloc"].sum()
unallocated_share = (unallocated_rev / rev_total * 100) if rev_total else 0.0

c1, c2, c3 = st.columns(3)
c1.metric("Unquoted Hours Share", f"{unquoted_share:.1f}%")
c2.metric("Unallocated Revenue Share", f"{unallocated_share:.1f}%")
c3.metric("Dept Mismatch Hours", f"{fact.loc[fact['dept_mismatch'], 'actual_hours'].sum():,.0f}")

st.caption("For deeper diagnostics, use the Portfolio Drivers and Job Drilldown pages.")
