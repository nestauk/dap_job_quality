import argparse
import pandas as pd
from tqdm import tqdm

from dap_job_quality import BUCKET_NAME, logging
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

    parser = argparse.ArgumentParser(
        description="Script to run with command line arguments."
    )

    parser.add_argument(
        "--production",
        default=False,
        type=bool,
        help="Run the script in production mode or test",
    )

    parser.add_argument(
        "--start_index",
        default=0,
        type=int,
        help="What index to start from",
    )

    args = parser.parse_args()
    logging.info(args)

    afs_raw_sample = pd.read_parquet(
        "s3://open-jobs-lake/job_quality/early_years/evaluation_sample/job_ads_by_sector_region_sample_size_21392.parquet"
    )
    afs_ids = afs_raw_sample["id"].unique().tolist()

    s3_keys = get_s3_data_paths(
        BUCKET_NAME,
        root="metaflow/data/ReedAdCurateFlow/1700394027436526/",
        file_types=["*.json"],
    )

    if not args.production:
        s3_keys = s3_keys[1400:1450]

    master_data_size = 0
    for i, key in tqdm(enumerate(s3_keys[args.start_index :])):
        temp_df = process_data(key, afs_ids)
        master_data_size += len(temp_df)

        save_to_s3(
            BUCKET_NAME,
            temp_df,
            f"job_quality/early_years/evaluation_sample/metadata_interim_production_{args.production}/job_ads_by_sector_region_sample_size_21392_metadata_chunk_{i+args.start_index}.parquet",
        )

    paths = get_s3_data_paths(
        BUCKET_NAME,
        root=f"job_quality/early_years/evaluation_sample/metadata_interim_production_{args.production}",
        file_types=["*.parquet"],
    )
    paths = ["s3://" + BUCKET_NAME + "/" + path for path in paths]
    master_df = pd.concat(pd.read_parquet(f) for f in paths)

    if master_data_size != len(master_df):
        logging.warning(
            f"Data size mismatch. Should be {master_data_size} but is actually {len(master_df)}"
        )
    else:
        logging.info(f"Final data comprises {len(master_df)} job ads")

    save_to_s3(
        BUCKET_NAME,
        master_df,
        f"job_quality/early_years/evaluation_sample/job_ads_by_sector_region_sample_size_21392_metadata_production_{args.production}.parquet",
    )
