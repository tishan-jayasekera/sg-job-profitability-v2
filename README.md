Social Garden PSA Analytics Engine

A Professional Services Automation (PSA) analytics platform that triangulates Revenue (Finance), Effort (Timesheets), and Scope (Quotes) to determine true job profitability.

Architecture

ETL Layer (src/etl/): Normalizes disparate Excel sheets into a unified Star Schema.

Analytics Layer (src/analytics/): Python engines that calculate Variance, Margin Erosion, and Burn Rates.

UI Layer (src/ui/): A Streamlit dashboard for visualization.

Setup

Place Quoted_Task_Report_FY26.xlsx in data/raw/.

Run the pipeline:

python -m src.etl.pipeline


Run the app:

streamlit run app.py
