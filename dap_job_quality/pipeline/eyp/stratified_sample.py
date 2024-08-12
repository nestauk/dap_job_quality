# import argparse
import pandas as pd

from dap_job_quality import PROJECT_DIR, BUCKET_NAME, logging
from dap_job_quality.getters.afs_data import get_sim_occ_ads, get_eyp_ads
from dap_job_quality.getters.data_getters import save_to_s3

SEED = 42

if __name__ == "__main__":
    # parser = argparse.ArgumentParser(
    #     description="Script to run with command line arguments."
    # )

    # parser.add_argument(
    #     "--sample_size",
    #     default=500,
    #     type=int,
    #     help="How many job ads do you want in your sample?",
    # )

    # args = parser.parse_args()
    # logging.info(args)

    eyp = get_eyp_ads()
    sim_occs = get_sim_occ_ads()
    all_job_ads = pd.concat([eyp, sim_occs], axis=0).drop_duplicates()

    all_job_ads = all_job_ads[
        all_job_ads["itl_1_name"] != "Northern Ireland"
    ]  # NI has fewer than 100

    all_job_ads["year"] = pd.to_datetime(all_job_ads["created"]).dt.year
    # Ideally we would stratify by ITL 1 as well but the minimum sample size ends up too small
    all_job_ads["stratify_col"] = (
        all_job_ads["year"].astype(str) + "_" + all_job_ads["sector"]
    )

    all_job_ads = all_job_ads[all_job_ads["itl_1_code"].notnull()]
    all_job_ads = all_job_ads[all_job_ads["year"] > 2020]
    logging.info(f"Total number of job ads (post 2020): {len(all_job_ads)}")

    # Identify and remove classes with fewer than two members
    value_counts = all_job_ads["stratify_col"].value_counts()
    to_keep = value_counts[value_counts > 2].index
    all_job_ads = all_job_ads[all_job_ads["stratify_col"].isin(to_keep)]

    grouped = all_job_ads.groupby("stratify_col")
    sample_size = min(grouped.size())  # Minimum group size to ensure equal samples
    logging.info(f"Minimum sample size: {sample_size}")

    sampled_df = grouped.apply(
        lambda x: x.sample(sample_size, random_state=SEED)
    ).reset_index(drop=True)

    sampled_df.drop(columns=["stratify_col"], inplace=True)
    sample_n_rows = len(sampled_df)
    logging.info(f"Size of sample: {sample_n_rows}")

    save_to_s3(
        BUCKET_NAME,
        sampled_df,
        f"job_quality/early_years/evaluation_sample/job_ads_sample_size_{sample_n_rows}.parquet",
    )
