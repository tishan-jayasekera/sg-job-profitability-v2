import streamlit as st
import pandas as pd
import os

from src.analytics.financial_engine import FinancialEngine
from src.analytics.variance_engine import VarianceEngine
from src.analytics.smart_builder import SmartBuilder

from src.ui.tabs import executive_summary, job_diagnosis, smart_quote

st.set_page_config(page_title="Social Garden PSA", layout="wide")

# --- DATA LOADER ---
@st.cache_resource
def load_data():
    fact_path = "data/processed/fact_job_task_month.csv"
    summary_path = "data/processed/job_task_summary.csv"
    
    if not os.path.exists(fact_path) or not os.path.exists(summary_path):
        st.error("Data not found. Please run `python -m src.etl.pipeline` first.")
        return None, None
        
    fact = pd.read_csv(fact_path)
    summary = pd.read_csv(summary_path)
    return fact, summary

fact_df, summary_df = load_data()

if fact_df is not None:
    # --- INITIALIZE ENGINES ---
    fin_engine = FinancialEngine(fact_df)
    var_engine = VarianceEngine(summary_df)
    build_engine = SmartBuilder(summary_df)

    # --- SIDEBAR ---
    st.sidebar.title("PSA Analytics")
    st.sidebar.info(f"Data Loaded: {len(summary_df):,} Tasks across {fact_df['job_no'].nunique():,} Jobs")

    # --- TABS ---
    tab1, tab2, tab3 = st.tabs(["Executive Pulse", "Job Diagnosis", "Smart Builder"])

    with tab1:
        executive_summary.render(fin_engine, var_engine)

    with tab2:
        # Get list of completed/active jobs for dropdown
        jobs = summary_df['job_no'].unique().tolist()
        jobs.sort()
        job_diagnosis.render(var_engine, jobs)

    with tab3:
        # Get list of products
        products = [p for p in summary_df['Product_quote'].dropna().unique() if p != "0"]
        products.sort()
        smart_quote.render(build_engine, products)