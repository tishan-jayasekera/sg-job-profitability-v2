import pandas as pd
import numpy as np


class VarianceEngine:
    def __init__(self, fact_df):
        self.df = fact_df.copy()

    def _job_task_rollup(self, job_id: str) -> pd.DataFrame:
        job_data = self.df[self.df["job_no"] == job_id].copy()
        job_data = job_data[job_data["task_name"] != "__UNALLOCATED__"]

        quote_fields = [
            "quoted_time",
            "quoted_amount",
            "Department_quote",
            "Product_quote",
            "Client_quote",
            "Job_Name_quote",
            "Job_Status_quote",
        ]

        quote_agg = job_data.groupby(["job_no", "task_name"], as_index=False)[quote_fields].max()
        actual_agg = (
            job_data.groupby(["job_no", "task_name"], as_index=False)
            .agg(
                actual_hours=("actual_hours", "sum"),
                actual_cost=("actual_cost", "sum"),
                revenue_allocated=("revenue_allocated", "sum"),
                Department_actual=("Department_actual", "first"),
            )
        )

        return actual_agg.merge(quote_agg, on=["job_no", "task_name"], how="left")

    def get_job_diagnosis(self, job_id):
        """Returns detailed variance breakdown for a specific job."""
        job_data = self._job_task_rollup(job_id)

        job_data["scope_variance"] = job_data["actual_hours"] - job_data["quoted_time"].fillna(0)
        job_data["price_recovery"] = job_data["revenue_allocated"] - job_data["quoted_amount"].fillna(0)
        return job_data

    def get_problem_jobs(self, min_revenue=1000):
        """Identifies jobs with significant margin erosion."""
        df = self.df.copy()
        df = df[df["task_name"] != "__UNALLOCATED__"]

        job_actuals = (
            df.groupby("job_no", as_index=False)
            .agg(
                revenue_allocated=("revenue_allocated", "sum"),
                actual_cost=("actual_cost", "sum"),
                actual_hours=("actual_hours", "sum"),
            )
        )

        job_quotes = (
            df.groupby(["job_no", "task_name"], as_index=False)
            .agg(
                quoted_time=("quoted_time", "max"),
                quoted_amount=("quoted_amount", "max"),
                Client_quote=("Client_quote", "max"),
                Job_Name_quote=("Job_Name_quote", "max"),
            )
        )
        job_quotes = job_quotes.groupby("job_no", as_index=False).agg(
            quoted_time=("quoted_time", "sum"),
            quoted_amount=("quoted_amount", "sum"),
            Client_quote=("Client_quote", "first"),
            Job_Name_quote=("Job_Name_quote", "first"),
        )

        jobs = job_actuals.merge(job_quotes, on="job_no", how="left")
        jobs = jobs[jobs["revenue_allocated"] > min_revenue]

        jobs["margin"] = jobs["revenue_allocated"] - jobs["actual_cost"]
        jobs["margin_pct"] = np.where(
            jobs["revenue_allocated"] > 0,
            (jobs["margin"] / jobs["revenue_allocated"]) * 100,
            0.0,
        )
        jobs["quote_gap"] = jobs["revenue_allocated"] - jobs["quoted_amount"].fillna(0)

        return jobs.sort_values("margin_pct").head(20)
