import plotly.express as px
import streamlit as st

from src.app_data import apply_filters, load_data, render_sidebar

st.set_page_config(page_title="Task Traceability", layout="wide")

data = load_data()
filters = render_sidebar(data["fact"])
filtered = apply_filters(data, filters)

fact = filtered["fact"]

st.title("Task Traceability")

job_options = ["Portfolio"] + sorted(fact["job_no"].dropna().unique().tolist())
selected_job = st.selectbox("Scope", job_options, index=0)

scope_df = fact if selected_job == "Portfolio" else fact[fact["job_no"] == selected_job]

if scope_df.empty:
    st.info("No data for selected scope.")
    st.stop()

st.subheader("Tasks Driving GP Loss")
loss_tasks = (
    scope_df.groupby("task_name", as_index=False)
    .agg(gp=("gp", "sum"), actual_hours=("actual_hours", "sum"), actual_cost=("actual_cost", "sum"))
    .sort_values("gp", ascending=True)
    .head(15)
)

st.dataframe(loss_tasks, width="stretch")

st.subheader("Hours vs GP (Task Scatter)")
fig = px.scatter(
    scope_df,
    x="actual_hours",
    y="gp",
    color="dept_match_status",
    hover_data=["task_name"],
)
st.plotly_chart(fig, use_container_width=True)

role_col = "Role_top"
if role_col in scope_df.columns:
    st.subheader("Role Concentration")
    role_summary = scope_df.groupby(role_col, as_index=False).agg(hours=("actual_hours", "sum"), gp=("gp", "sum"))
    role_summary = role_summary.sort_values("hours", ascending=False).head(10)
    st.dataframe(role_summary, width="stretch")
