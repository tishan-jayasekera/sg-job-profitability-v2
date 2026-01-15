import pandas as pd

from src.utils import normalize_job_no, normalize_text, to_month_key


def is_truthy_excluded(value: object) -> bool:
    text = normalize_text(value).upper()
    return text in {"Y", "YES", "TRUE", "1", "EXCLUDE", "EXCLUDED"} or value is True


def build_revenue_monthly(df: pd.DataFrame) -> pd.DataFrame:
    data = df.copy()
    data["job_no"] = data["Job Number"].map(normalize_job_no)
    data["month_key"] = to_month_key(data["Month"])
    data["excluded_flag"] = data["Excluded"].map(is_truthy_excluded)
    data["amount"] = pd.to_numeric(data["Amount"], errors="coerce").fillna(0.0)

    data = data[~data["excluded_flag"]]
    revenue_monthly = (
        data.groupby(["job_no", "month_key"], as_index=False)
        .agg(
            revenue_monthly=("amount", "sum"),
            FY=("FY", "first"),
        )
    )
    return revenue_monthly
