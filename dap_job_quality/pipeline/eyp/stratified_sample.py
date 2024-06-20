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
        default=100,
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

    # Identify and remove classes with fewer than two members
    value_counts = all_job_ads["stratify_col"].value_counts()
    to_keep = value_counts[value_counts > 2].index
    all_job_ads = all_job_ads[all_job_ads["stratify_col"].isin(to_keep)]

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

    save_to_s3(
        BUCKET_NAME,
        stratified_sample,
        f"job_quality/early_years/evaluation_sample/job_ads_sample_{args.sample_size}.parquet",
    )
