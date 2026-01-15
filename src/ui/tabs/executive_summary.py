import streamlit as st
from src.ui.components import financial_trend_chart

def render(financial_engine, variance_engine):
    st.header("Executive Summary")
    
    kpis = financial_engine.get_kpis()
    
    # Metric Cards
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Revenue (Allocated)", f"${kpis['Total Revenue']:,.0f}")
    c2.metric("Total Cost", f"${kpis['Total Cost']:,.0f}")
    c3.metric("Net Margin", f"{kpis['Margin %']:.1f}%")
    c4.metric("Unallocated Revenue", f"${kpis['Unallocated Revenue']:,.0f}", delta_color="inverse")
    
    st.subheader("Financial Trend")
    trend_df = financial_engine.get_monthly_trend()
    st.altair_chart(financial_trend_chart(trend_df), use_container_width=True)
    
    st.subheader("Bottom 10 Jobs (Margin %)")
    problems = variance_engine.get_problem_jobs()
    st.dataframe(problems[['job_no', 'Client_quote', 'revenue_allocated', 'margin_pct', 'quote_gap']].style.format({
        'revenue_allocated': '${:,.0f}',
        'margin_pct': '{:.1f}%',
        'quote_gap': '${:,.0f}'
    }))