import pandas as pd


def get_keywords():
    return pd.read_csv(
        "s3://open-jobs-lake/job_quality/keywords/keyword_lookup - v7.csv"
    )
