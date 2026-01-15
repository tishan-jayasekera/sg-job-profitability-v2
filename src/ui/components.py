import altair as alt
import streamlit as st

def variance_bar_chart(df):
    """Generates a diverging bar chart for Scope Variance."""
    chart = alt.Chart(df).mark_bar().encode(
        x=alt.X('scope_variance:Q', title='Hours Variance (Actual - Quote)'),
        y=alt.Y('task_name:N', sort='-x', title=None),
        color=alt.condition(
            alt.datum.scope_variance > 0,
            alt.value("#d9534f"),  # Red for overrun
            alt.value("#5cb85c")   # Green for under budget
        ),
        tooltip=['task_name', 'quoted_time', 'actual_hours', 'scope_variance']
    ).properties(height=max(300, len(df)*30))
    return chart

def financial_trend_chart(df):
    """Line chart for Revenue vs Cost."""
    base = alt.Chart(df).encode(x='month_key:T')
    
    rev = base.mark_line(color='#4c78a8').encode(y='revenue_allocated', tooltip=['month_key', 'revenue_allocated'])
    cost = base.mark_line(color='#e45756').encode(y='total_cost', tooltip=['month_key', 'total_cost'])
    
    return (rev + cost).properties(height=300)