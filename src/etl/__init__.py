import pandas as pd
import re
import numpy as np

def norm_str(x: object) -> str:
    """Normalize string-ish values (strip + collapse whitespace)."""
    if pd.isna(x):
        return ""
    s = str(x).strip()
    s = re.sub(r"\s+", " ", s)
    return s

def clean_job_no(x: object) -> str:
    return norm_str(x).upper()

def clean_task_name(x: object) -> str:
    """Task key: normalized, but preserve human readability."""
    s = norm_str(x)
    return s.casefold()

def clean_dept(x: object) -> str:
    return norm_str(x).upper()

def truthy_excluded(x: object) -> bool:
    s = norm_str(x).upper()
    return s in {"Y", "YES", "TRUE", "1", "EXCLUDE", "EXCLUDED"} or x is True

def month_key_first_of_month(d: pd.Series) -> pd.Series:
    dt = pd.to_datetime(d, errors="coerce")
    return dt.dt.to_period("M").dt.to_timestamp()

def weighted_mode(values: pd.Series, weights: pd.Series) -> tuple:
    """Return (top_value, top_share, {value: weight_sum}). Ignores blanks."""
    tmp = pd.DataFrame({"v": values.fillna("").astype(str), "w": pd.to_numeric(weights, errors="coerce").fillna(0.0)})
    tmp["v"] = tmp["v"].map(norm_str)
    tmp = tmp[tmp["v"] != ""]
    if tmp.empty:
        return ("", 0.0, {})
    dist = tmp.groupby("v", as_index=False)["w"].sum().sort_values("w", ascending=False)
    top_v = str(dist.iloc[0]["v"])
    total = float(dist["w"].sum())
    top_share = float(dist.iloc[0]["w"] / total) if total else 0.0
    return (top_v, top_share, {})