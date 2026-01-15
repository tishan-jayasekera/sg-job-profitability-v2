import pandas as pd

from src.clean import map_departments, map_task_names, standardize_keys
from src.utils import normalize_text, to_month_key, weighted_mode


def _is_truthy(value: object) -> bool:
    return normalize_text(value).upper() in {"Y", "YES", "TRUE", "1"}


def _weighted_attribute(group: pd.DataFrame, column: str, weight_col: str = "hours") -> pd.Series:
    top_value, top_share = weighted_mode(group[column], group[weight_col])
    distinct_count = group[column].fillna("").astype(str).nunique()
    mixed_flag = 1 if (distinct_count > 1 and top_share < 0.7) else 0
    return pd.Series({
        f"{column}_top": top_value,
        f"{column}_top_share": top_share,
        f"{column}_mixed": mixed_flag,
    })


def build_timesheet_task_month(df: pd.DataFrame) -> pd.DataFrame:
    data = df.copy()
    data = standardize_keys(data, "[Job] Job No.", "[Job Task] Name")
    data = map_task_names(data)

    data["month_key"] = pd.to_datetime(data.get("Month Key"), errors="coerce")
    missing = data["month_key"].isna()
    if missing.any():
        data.loc[missing, "month_key"] = to_month_key(data.loc[missing, "[Time] Date"])

    data["hours"] = pd.to_numeric(data["[Time] Time"], errors="coerce").fillna(0.0).clip(lower=0)
    data["base_rate"] = pd.to_numeric(data["[Task] Base Rate"], errors="coerce").fillna(0.0)
    data["billable_rate"] = pd.to_numeric(data["[Task] Billable Rate"], errors="coerce").fillna(0.0)
    data["cost"] = data["hours"] * data["base_rate"]

    data["billable_flag"] = data.get("Billable?").map(_is_truthy)
    data["billable_hours"] = data["hours"].where(data["billable_flag"], 0.0)

    data["onshore_flag"] = data.get("Onshore").map(_is_truthy)
    data["onshore_hours"] = data["hours"].where(data["onshore_flag"], 0.0)

    data = map_departments(data, "Department")
    data = data.rename(columns={"Department": "Department_actual_raw"})
    data["Department_actual"] = data["Department_actual_raw"]

    def agg_group(group: pd.DataFrame) -> pd.Series:
        total_hours = group["hours"].sum()
        billable_hours = group["billable_hours"].sum()
        onshore_hours = group["onshore_hours"].sum()
        total_cost = group["cost"].sum()
        avg_base_rate = (group["base_rate"] * group["hours"]).sum() / total_hours if total_hours else 0.0
        avg_billable_rate = (group["billable_rate"] * group["hours"]).sum() / total_hours if total_hours else 0.0

        dept_stats = _weighted_attribute(group, "Department_actual")
        role_stats = _weighted_attribute(group, "Role") if "Role" in group else pd.Series()
        category_col = "[Category] Category"
        category_stats = _weighted_attribute(group, category_col) if category_col in group else pd.Series()
        deliverable_stats = _weighted_attribute(group, "Deliverable") if "Deliverable" in group else pd.Series()
        function_stats = _weighted_attribute(group, "Function") if "Function" in group else pd.Series()

        return pd.concat([
            pd.Series({
                "total_hours": total_hours,
                "billable_hours": billable_hours,
                "onshore_hours": onshore_hours,
                "total_cost": total_cost,
                "avg_base_rate": avg_base_rate,
                "avg_billable_rate": avg_billable_rate,
                "distinct_staff_count": group["[Staff] Name"].nunique(),
            }),
            dept_stats,
            role_stats,
            category_stats,
            deliverable_stats,
            function_stats,
        ])

    grouped = (
        data.groupby(["job_no", "task_name", "month_key"], as_index=False)
        .apply(agg_group)
        .reset_index()
    )

    grouped = grouped.rename(columns={
        "total_hours": "actual_hours",
        "total_cost": "actual_cost",
    })

    raw_keys = data.groupby(["job_no", "task_name", "month_key"], as_index=False).agg(
        task_name_raw=("task_name_raw", "first"),
        job_no_raw=("job_no_raw", "first"),
    )
    grouped = grouped.merge(raw_keys, on=["job_no", "task_name", "month_key"], how="left")
    if "Department_actual_top" in grouped.columns:
        grouped["Department_actual"] = grouped["Department_actual_top"]

    return grouped
