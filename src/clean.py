import pandas as pd

from src.utils import (
    apply_mapping,
    load_mapping,
    normalize_department,
    normalize_job_no,
    normalize_task_name,
    normalize_text,
)


def standardize_keys(df: pd.DataFrame, job_col: str, task_col: str) -> pd.DataFrame:
    df = df.copy()
    df["job_no_raw"] = df[job_col]
    df["task_name_raw"] = df[task_col]
    df["job_no"] = df[job_col].map(normalize_job_no)
    df["task_name"] = df[task_col].map(normalize_task_name)
    return df


def map_task_names(df: pd.DataFrame, mapping_path: str = "config/task_name_map.csv") -> pd.DataFrame:
    df = df.copy()
    mapping = load_mapping(mapping_path, "raw_task_name", "task_name")
    if mapping:
        df["task_name"] = apply_mapping(df["task_name_raw"], mapping).map(normalize_task_name)
    return df


def map_departments(df: pd.DataFrame, column: str, mapping_path: str = "config/department_map.csv") -> pd.DataFrame:
    df = df.copy()
    mapping = load_mapping(mapping_path, "raw_department", "department")
    if mapping:
        df[column] = apply_mapping(df[column], mapping)
    df[column] = df[column].map(normalize_department)
    return df


def normalize_columns(df: pd.DataFrame, columns) -> pd.DataFrame:
    df = df.copy()
    for col in columns:
        if col in df.columns:
            df[col] = df[col].map(normalize_text)
    return df
