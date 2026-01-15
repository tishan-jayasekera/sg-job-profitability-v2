import pandas as pd

from src.clean import map_departments, map_task_names, standardize_keys
from src.utils import normalize_text, to_month_key


def build_quote_task(df: pd.DataFrame) -> pd.DataFrame:
    data = df.copy()
    data = standardize_keys(data, "[Job] Job No.", "[Job Task] Name")
    data = map_task_names(data)

    data["quoted_time"] = pd.to_numeric(data["[Job Task] Quoted Time"], errors="coerce").fillna(0.0)
    data["quoted_amount"] = pd.to_numeric(data["[Job Task] Quoted Amount"], errors="coerce").fillna(0.0)

    data = map_departments(data, "Department")
    data["Department_quote"] = data["Department"]

    data["Product"] = data.get("Product").map(normalize_text)
    data["Client"] = data.get("[Job] Client").map(normalize_text)
    data["Job_Category"] = data.get("[Job] Category").map(normalize_text)
    data["Job_Status"] = data.get("[Job] Status").map(normalize_text)
    data["Job_Name"] = data.get("[Job] Name").map(normalize_text)

    start_date = data.get("[Job Task] Start Date").fillna(data.get("[Job] Start Date"))
    due_date = data.get("[Job Task] Due Date").fillna(data.get("[Job] Due Date"))
    data["quote_month_key"] = to_month_key(start_date.fillna(due_date))

    quote_task = (
        data.groupby(["job_no", "task_name"], as_index=False)
        .agg(
            quoted_time=("quoted_time", "sum"),
            quoted_amount=("quoted_amount", "sum"),
            Department_quote=("Department_quote", "first"),
            Product=("Product", "first"),
            Client=("Client", "first"),
            Job_Category=("Job_Category", "first"),
            Job_Status=("Job_Status", "first"),
            Job_Name=("Job_Name", "first"),
            quote_month_key=("quote_month_key", "first"),
        )
    )

    return quote_task
