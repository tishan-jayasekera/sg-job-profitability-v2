import json
from itertools import combinations

import pandas as pd


def build_job_comps_index(fact: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    df = fact.copy()
    df = df[df["task_name"] != "__UNALLOCATED__"]

    job_meta = (
        df.groupby("job_no", as_index=False)
        .agg(
            dept=("Department_reporting", "first"),
            Product=("Product", "first"),
        )
    )

    job_tasks = (
        df.groupby(["job_no", "task_name"], as_index=False)
        .agg(hours=("actual_hours", "sum"))
    )
    job_task_sets = job_tasks[job_tasks["hours"] > 0].groupby("job_no")

    task_sets = {job: set(group["task_name"].tolist()) for job, group in job_task_sets}

    comps = []
    for (dept, product), group in job_meta.groupby(["dept", "Product"]):
        jobs = group["job_no"].tolist()
        for job in jobs:
            base_set = task_sets.get(job, set())
            scores = []
            for other in jobs:
                if other == job:
                    continue
                other_set = task_sets.get(other, set())
                if not base_set and not other_set:
                    score = 0.0
                else:
                    score = len(base_set & other_set) / len(base_set | other_set) if (base_set | other_set) else 0.0
                scores.append((other, score))
            scores.sort(key=lambda x: x[1], reverse=True)
            comps.append({
                "job_no": job,
                "dept": dept,
                "Product": product,
                "comps": json.dumps(scores[:top_n]),
            })

    return pd.DataFrame(comps)
