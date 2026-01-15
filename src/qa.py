import pandas as pd

from src.utils import current_timestamp


def run_qa(fact: pd.DataFrame) -> dict:
    qa = {
        "timestamp": current_timestamp(),
        "checks": {},
        "warnings": [],
    }

    key_cols = ["job_no", "task_name", "month_key"]
    duplicates = fact.duplicated(subset=key_cols).sum()
    qa["checks"]["fact_unique_keys"] = int(duplicates == 0)
    qa["checks"]["fact_duplicate_count"] = int(duplicates)

    recon = (
        fact.groupby(["job_no", "month_key"], as_index=False)
        .agg(revenue_monthly=("revenue_monthly", "sum"), revenue_allocated=("revenue_allocated", "sum"))
    )
    recon["delta"] = (recon["revenue_monthly"] - recon["revenue_allocated"]).abs()
    qa["checks"]["revenue_allocation_pass"] = int((recon["delta"] < 1e-6).all())
    qa["checks"]["revenue_allocation_fail_count"] = int((recon["delta"] >= 1e-6).sum())

    negative_hours = (fact["actual_hours"] < 0).sum()
    qa["checks"]["negative_hours_count"] = int(negative_hours)

    missing_dept_actual = fact[(fact["actual_hours"] > 0) & (fact["Department_actual"].fillna("") == "")].shape[0]
    qa["checks"]["missing_department_actual_count"] = int(missing_dept_actual)

    return qa
