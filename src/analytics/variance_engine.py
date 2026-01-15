import pandas as pd
import numpy as np

class VarianceEngine:
    def __init__(self, summary_df):
        self.df = summary_df

    def get_job_diagnosis(self, job_id):
        """Returns detailed variance breakdown for a specific job."""
        job_data = self.df[self.df['job_no'] == job_id].copy()
        
        # Calculate key variances
        job_data['scope_variance'] = job_data['actual_hours'] - job_data['quoted_time']
        job_data['price_recovery'] = job_data['revenue_allocated'] - job_data['quoted_amount']
        
        return job_data

    def get_problem_jobs(self, min_revenue=1000):
        """Identifies jobs with significant margin erosion."""
        # Aggregating task data up to job level
        jobs = self.df.groupby('job_no').agg({
            'Job_Name_quote': 'first',
            'Client_quote': 'first',
            'revenue_allocated': 'sum',
            'actual_cost': 'sum',
            'quoted_amount': 'sum',
            'quoted_time': 'sum',
            'actual_hours': 'sum'
        }).reset_index()
        
        jobs = jobs[jobs['revenue_allocated'] > min_revenue]
        
        jobs['margin'] = jobs['revenue_allocated'] - jobs['actual_cost']
        jobs['margin_pct'] = (jobs['margin'] / jobs['revenue_allocated']) * 100
        jobs['quote_gap'] = jobs['revenue_allocated'] - jobs['quoted_amount']
        
        # Sort by lowest margin %
        return jobs.sort_values('margin_pct').head(20)