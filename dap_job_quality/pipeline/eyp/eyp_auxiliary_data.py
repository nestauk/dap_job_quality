import pandas as pd
import re

from dap_job_quality import BUCKET_NAME
from dap_job_quality.getters.data_getters import (
    save_to_s3,
    load_s3_json,
    get_s3_data_paths,
)


def process_data(s3_key, afs_ids):
    data = load_s3_json(BUCKET_NAME, s3_key)
    temp_df = pd.DataFrame(data)
    temp_df["id"] = pd.to_numeric(temp_df["id"], downcast="integer")
    temp_df = temp_df[temp_df["id"].isin(afs_ids)]
    print(f"Number of ads retrieved from {s3_key}: \n {len(temp_df)}")
    print(len(temp_df))
    temp_df["raw_salary_float"] = temp_df["raw_salary"].astype(float)
    temp_df["raw_min_salary_float"] = temp_df["raw_min_salary"].astype(float)
    temp_df["raw_max_salary_float"] = temp_df["raw_max_salary"].astype(float)
    return temp_df


if __name__ == "__main__":

    afs_raw_sample = pd.read_parquet(
        "s3://open-jobs-lake/job_quality/early_years/evaluation_sample/job_ads_sample_size_16392.parquet"
    )
    afs_ids = afs_raw_sample["id"].unique().tolist()

    s3_keys = get_s3_data_paths(
        BUCKET_NAME,
        root="metaflow/data/ReedAdCurateFlow/1700394027436526/",
        file_types=["*.json"],
    )

    master_df = pd.DataFrame()

    for key in s3_keys:
        temp_df = process_data(key, afs_ids)

        pattern = r"(\d{8}-\d{8})"
        match = re.search(pattern, key)
        start_finish_ids = match.group(1)

        save_to_s3(
            BUCKET_NAME,
            temp_df,
            f"job_quality/early_years/evaluation_sample/metadata_interim/job_ads_sample_size_16392_metadata_{start_finish_ids}.parquet",
        )

        master_df = pd.concat([master_df, temp_df])

    save_to_s3(
        BUCKET_NAME,
        master_df,
        f"job_quality/early_years/evaluation_sample/job_ads_sample_size_16392_metadata.parquet",
    )
