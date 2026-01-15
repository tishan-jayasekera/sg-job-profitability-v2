import pandas as pd


class FinancialEngine:
    def __init__(self, fact_df):
        self.df = fact_df.copy()
        if "month_key" in self.df.columns:
            self.df["month_key"] = pd.to_datetime(self.df["month_key"], errors="coerce")

    def get_kpis(self):
        revenue = self.df["revenue_allocated"].sum() if "revenue_allocated" in self.df.columns else 0.0
        cost = self.df["actual_cost"].sum() if "actual_cost" in self.df.columns else 0.0
        margin_pct = ((revenue - cost) / revenue * 100) if revenue else 0.0

        unallocated = 0.0
        if "task_name" in self.df.columns and "revenue_allocated" in self.df.columns:
            unallocated = self.df.loc[self.df["task_name"] == "__UNALLOCATED__", "revenue_allocated"].sum()

        return {
            "Total Revenue": revenue,
            "Total Cost": cost,
            "Margin %": margin_pct,
            "Unallocated Revenue": unallocated,
        }

    def get_monthly_trend(self):
        if "month_key" not in self.df.columns:
            return pd.DataFrame(columns=["month_key", "revenue_allocated", "actual_cost"])

        trend = (
            self.df.groupby("month_key", as_index=False)
            .agg(revenue_allocated=("revenue_allocated", "sum"),
                 actual_cost=("actual_cost", "sum"))
            .sort_values("month_key")
        )
        return trend
