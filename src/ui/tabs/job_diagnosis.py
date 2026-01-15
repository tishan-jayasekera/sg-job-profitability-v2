import streamlit as st
from src.ui.components import variance_bar_chart

def render(variance_engine, job_list):
    st.header("Job Diagnosis")
    
    selected_job = st.selectbox("Select Job to Diagnose", job_list)
    
    if selected_job:
        job_data = variance_engine.get_job_diagnosis(selected_job)
        
        # Summary Metrics
        total_rev = job_data['revenue_allocated'].sum()
        total_quote = job_data['quoted_amount'].sum()
        total_actual_hrs = job_data['actual_hours'].sum()
        total_quoted_hrs = job_data['quoted_time'].sum()
        
        c1, c2, c3 = st.columns(3)
        c1.metric("Revenue vs Quote", f"${total_rev:,.0f}", delta=f"${total_rev-total_quote:,.0f}")
        c2.metric("Hours vs Quote", f"{total_actual_hrs:,.1f}", delta=f"{total_actual_hrs-total_quoted_hrs:,.1f}", delta_color="inverse")
        
        st.subheader("Scope Variance (The Triangle)")
        st.caption("Right (Red) = Overrun | Left (Green) = Efficiency")
        st.altair_chart(variance_bar_chart(job_data), width="stretch")
        
        st.subheader("Task Details")
        st.dataframe(job_data)
