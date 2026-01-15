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

    if not os.path.exists(fact_path):
        st.error("Data not found. Please run `python -m src.etl.pipeline` first.")
        return None

    fact = pd.read_csv(fact_path)
    return fact

fact_df = load_data()

if fact_df is not None:
    # --- INITIALIZE ENGINES ---
    fin_engine = FinancialEngine(fact_df)
    var_engine = VarianceEngine(fact_df)
    build_engine = SmartBuilder(fact_df)

    # --- SIDEBAR ---
    st.sidebar.title("PSA Analytics")
    task_count = fact_df[fact_df["task_name"] != "__UNALLOCATED__"]["task_name"].nunique()
    st.sidebar.info(f"Data Loaded: {task_count:,} Tasks across {fact_df['job_no'].nunique():,} Jobs")

    # --- TABS ---
    tab1, tab2, tab3 = st.tabs(["Executive Pulse", "Job Diagnosis", "Smart Builder"])

    with tab1:
        executive_summary.render(fin_engine, var_engine)

    with tab2:
        # Get list of completed/active jobs for dropdown
        jobs = fact_df.loc[fact_df["task_name"] != "__UNALLOCATED__", "job_no"].dropna().unique().tolist()
        jobs.sort()
        job_diagnosis.render(var_engine, jobs)

    with tab3:
        # Get list of products
        products = [p for p in fact_df["Product_quote"].dropna().unique() if p != "0"]
        products.sort()
        smart_quote.render(build_engine, products)
