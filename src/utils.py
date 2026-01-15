import json
import logging
import os
from datetime import datetime
from typing import Dict, Tuple

import numpy as np
import pandas as pd
import yaml


def setup_logger(name: str = "job_profitability", level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    log_level = getattr(logging, level.upper(), logging.INFO)
    logger.setLevel(log_level)
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def read_settings(path: str = "config/settings.yaml") -> Dict:
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def safe_divide(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return numerator / denominator


def to_month_key(series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series, errors="coerce")
    return dt.dt.to_period("M").dt.to_timestamp()


def normalize_text(value: object) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    return " ".join(text.split())


def normalize_job_no(value: object) -> str:
    return normalize_text(value).upper()


def normalize_task_name(value: object) -> str:
    return normalize_text(value).lower()


def normalize_department(value: object) -> str:
    return normalize_text(value).upper()


def weighted_mode(values: pd.Series, weights: pd.Series) -> Tuple[str, float]:
    tmp = pd.DataFrame({"v": values.fillna("").astype(str), "w": pd.to_numeric(weights, errors="coerce").fillna(0.0)})
    tmp["v"] = tmp["v"].map(normalize_text)
    tmp = tmp[tmp["v"] != ""]
    if tmp.empty:
        return "", 0.0
    dist = tmp.groupby("v", as_index=False)["w"].sum().sort_values("w", ascending=False)
    top_value = str(dist.iloc[0]["v"])
    total = float(dist["w"].sum())
    share = float(dist.iloc[0]["w"] / total) if total else 0.0
    return top_value, share


def load_mapping(path: str, key_col: str, value_col: str) -> Dict[str, str]:
    if not os.path.exists(path):
        return {}
    df = pd.read_csv(path)
    if key_col not in df.columns or value_col not in df.columns:
        return {}
    mapping = {}
    for _, row in df.iterrows():
        raw = normalize_text(row.get(key_col, ""))
        target = normalize_text(row.get(value_col, ""))
        if raw:
            mapping[raw] = target
    return mapping


def apply_mapping(series: pd.Series, mapping: Dict[str, str]) -> pd.Series:
    if not mapping:
        return series
    return series.map(lambda x: mapping.get(normalize_text(x), normalize_text(x)))


def write_json(payload: Dict, path: str) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=str)


def parse_period_label(start: pd.Timestamp, end: pd.Timestamp) -> str:
    if pd.isna(start) or pd.isna(end):
        return "ALL"
    return f"{start.strftime('%Y-%m')}_to_{end.strftime('%Y-%m')}"


def current_timestamp() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def fiscal_year_label(dates: pd.Series) -> pd.Series:
    dt = pd.to_datetime(dates, errors="coerce")
    fy_year = dt.dt.year + (dt.dt.month >= 7).astype(int)
    return "FY" + fy_year.astype("Int64").astype(str).str[-2:]
