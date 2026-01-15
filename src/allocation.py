import pandas as pd
import numpy as np


def allocate_revenue(timesheet_task_month: pd.DataFrame, revenue_monthly: pd.DataFrame) -> pd.DataFrame:
    tm = timesheet_task_month.merge(revenue_monthly, how="left", on=["job_no", "month_key"])
    tm["revenue_monthly"] = tm["revenue_monthly"].fillna(0.0)

    job_month_totals = tm.groupby(["job_no", "month_key"], as_index=False).agg(
        total_job_hours=("actual_hours", "sum")
    )
    tm = tm.merge(job_month_totals, on=["job_no", "month_key"], how="left")
    tm["total_job_hours"] = tm["total_job_hours"].fillna(0.0)

    tm["task_share"] = np.where(
        tm["total_job_hours"] > 0,
        tm["actual_hours"] / tm["total_job_hours"],
        0.0,
    )
    tm["revenue_allocated"] = tm["task_share"] * tm["revenue_monthly"]
    tm["is_unallocated_row"] = False

    rev_only = revenue_monthly.merge(job_month_totals, on=["job_no", "month_key"], how="left")
    rev_only["total_job_hours"] = rev_only["total_job_hours"].fillna(0.0)
    unallocated = rev_only[(rev_only["revenue_monthly"] != 0) & (rev_only["total_job_hours"] <= 0)].copy()

    if not unallocated.empty:
        unallocated_rows = unallocated.assign(
            task_name="__UNALLOCATED__",
            revenue_allocated=lambda x: x["revenue_monthly"],
            task_share=0.0,
            is_unallocated_row=True,
        )
        for col in tm.columns:
            if col not in unallocated_rows.columns:
                unallocated_rows[col] = 0.0 if tm[col].dtype.kind in {"i", "u", "f"} else ""
        unallocated_rows = unallocated_rows[tm.columns]
        tm = pd.concat([tm, unallocated_rows], ignore_index=True)

    return tm
