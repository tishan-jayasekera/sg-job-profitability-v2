import json
import os

import pandas as pd
import streamlit as st

from src.app_data import apply_filters, load_data, render_sidebar

st.set_page_config(page_title="Data QA", layout="wide")

data = load_data()
filters = render_sidebar(data["fact"])
filtered = apply_filters(data, filters)

fact = filtered["fact"]

st.title("Data QA")

qa_path = "data/processed/qa_report.json"
if os.path.exists(qa_path):
    with open(qa_path, "r", encoding="utf-8") as handle:
        qa = json.load(handle)
    st.subheader("QA Summary")
    qa_checks = pd.DataFrame.from_dict(qa.get("checks", {}), orient="index", columns=["value"]).reset_index()
    qa_checks = qa_checks.rename(columns={"index": "check"})
    st.dataframe(qa_checks, width="stretch")
else:
    st.warning("qa_report.json not found. Run the build script to generate QA outputs.")

st.subheader("Department Mismatch Matrix")
if not fact.empty:
    matrix = pd.crosstab(fact["Department_actual"], fact["Department_quote"]).head(20)
    st.dataframe(matrix, width="stretch")

st.subheader("Coverage Stats")
coverage = {
    "Missing Department Actual": int((fact["Department_actual"].fillna("") == "").sum()),
    "Missing Department Quote": int((fact["Department_quote"].fillna("") == "").sum()),
    "Unquoted Tasks": int(fact["is_unquoted_task"].sum()),
    "Quote-Only Tasks": int(fact["is_quote_only_task"].sum()),
    "Unallocated Rows": int(fact["is_unallocated_row"].sum()),
}
coverage_df = pd.DataFrame(list(coverage.items()), columns=["metric", "count"])
st.dataframe(coverage_df, width="stretch")
