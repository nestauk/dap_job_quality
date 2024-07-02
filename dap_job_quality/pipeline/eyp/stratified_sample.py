import argparse
import pandas as pd
from sklearn.model_selection import StratifiedShuffleSplit

from dap_job_quality import PROJECT_DIR, BUCKET_NAME, logging
from dap_job_quality.getters.afs_data import get_sim_occ_ads, get_eyp_ads
from dap_job_quality.getters.data_getters import save_to_s3

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Script to run with command line arguments."
    )

    parser.add_argument(
        "--sample_size",
        default=500,
        type=int,
        help="How many job ads do you want in your sample?",
    )

    args = parser.parse_args()
    logging.info(args)

    eyp = get_eyp_ads()
    sim_occs = get_sim_occ_ads()
    all_job_ads = pd.concat([eyp, sim_occs], axis=0).drop_duplicates()

    all_job_ads["year"] = pd.to_datetime(all_job_ads["created"]).dt.year
    all_job_ads["stratify_col"] = (
        all_job_ads["year"].astype(str)
        + "_"
        + all_job_ads["sector"]
        + "_"
        + all_job_ads["itl_1_code"]
    )

    all_job_ads = all_job_ads[all_job_ads["itl_1_code"].notnull()]
    all_job_ads = all_job_ads[all_job_ads["year"] > 2020]
    logging.info(f"Total number of job ads (post 2020): {len(all_job_ads)}")

    # Identify and remove classes with fewer than two members
    value_counts = all_job_ads["stratify_col"].value_counts()
    to_keep = value_counts[value_counts > 2].index
    all_job_ads = all_job_ads[all_job_ads["stratify_col"].isin(to_keep)]

    if (args.sample_size - len(all_job_ads["stratify_col"].unique())) < 0:
        raise ValueError(
            "The sample is not big enough to accommodate the stratification. Choose a larger sample size."
        )

    # Define the stratified splitter
    splitter = StratifiedShuffleSplit(
        n_splits=1, test_size=args.sample_size, random_state=42
    )

    # Perform the stratified sampling
    for train_index, test_index in splitter.split(
        all_job_ads, all_job_ads["stratify_col"]
    ):
        stratified_sample = all_job_ads.iloc[test_index]

    stratified_sample.drop(columns=["stratify_col"], inplace=True)
    sample_n_rows = len(stratified_sample)
    logging.info(f"Size of sample: {sample_n_rows}")

    save_to_s3(
        BUCKET_NAME,
        stratified_sample,
        f"job_quality/early_years/evaluation_sample/job_ads_sample_size_{sample_n_rows}.parquet",
    )
