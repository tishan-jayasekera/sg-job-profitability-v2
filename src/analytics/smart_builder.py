import pandas as pd


class SmartBuilder:
    def __init__(self, fact_df):
        self.df = fact_df.copy()

    def get_product_benchmarks(self, product_name):
        """
        Analyzes historical performance for a product line to suggest hours.
        """
        df = self.df.copy()
        df = df[df["task_name"] != "__UNALLOCATED__"]

        job_task = (
            df.groupby(["job_no", "task_name"], as_index=False)
            .agg(
                actual_hours=("actual_hours", "sum"),
                quoted_time=("quoted_time", "max"),
                Product_quote=("Product_quote", "max"),
            )
        )

        cohort = job_task[
            (job_task["Product_quote"] == product_name)
            & (job_task["quoted_time"] > 0)
        ].copy()

        if cohort.empty:
            return pd.DataFrame()

        cohort["realization_factor"] = cohort["actual_hours"] / cohort["quoted_time"]

        stats = (
            cohort.groupby("task_name", as_index=False)
            .agg(
                avg_hours=("actual_hours", "mean"),
                median_hours=("actual_hours", "median"),
                risk_factor=("realization_factor", "median"),
                frequency=("job_no", "count"),
            )
        )

        stats = stats[stats["frequency"] > 2].sort_values("frequency", ascending=False)
        return stats
