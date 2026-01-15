import pandas as pd

class SmartBuilder:
    def __init__(self, summary_df):
        self.df = summary_df

    def get_product_benchmarks(self, product_name):
        """
        Analyzes historical performance for a product line to suggest hours.
        """
        # Filter for relevant tasks
        cohort = self.df[
            (self.df['Product_quote'] == product_name) & 
            (self.df['quoted_time'] > 0)
        ].copy()
        
        if cohort.empty:
            return pd.DataFrame()

        # Calculate Realization Factor (Actual / Quote)
        cohort['realization_factor'] = cohort['actual_hours'] / cohort['quoted_time']
        
        stats = cohort.groupby('task_name').agg({
            'actual_hours': ['mean', 'median'],
            'realization_factor': 'median',
            'job_no': 'count'
        }).reset_index()
        
        # Flatten columns
        stats.columns = ['task_name', 'avg_hours', 'median_hours', 'risk_factor', 'frequency']
        
        # Filter out rare tasks
        stats = stats[stats['frequency'] > 2].sort_values('frequency', ascending=False)
        
        return stats