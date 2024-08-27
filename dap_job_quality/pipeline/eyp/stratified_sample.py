# import argparse
import pandas as pd

from dap_job_quality import PROJECT_DIR, BUCKET_NAME, logging
from dap_job_quality.getters.afs_data import get_sim_occ_ads, get_eyp_ads
from dap_job_quality.getters.data_getters import save_to_s3

SEED = 42

GEO_MAPPING = {
    "north": [
        "North East (England)",
        "North West (England)",
        "Yorkshire and the Humber",
    ],
    "midlands": ["West Midlands (England)", "East Midlands (England)"],
    "south": ["South East (England)", "South West (England)", "East of England"],
    "london": ["London"],
}

SECTOR_MAPPING = {
    "eyp": ["Early Years Practitioner"],
    "other teaching": [
        "Teaching Assistant ",
        "Supply Teacher",
        "Primary School Teacher",
        "Special Needs Teacher",
        "Secondary School Teacher",
    ],
    "non education": ["Retail Assistant", "Waiter"],
}


def map_groups(name, mapping_dict):
    for key, names in mapping_dict.items():
        if name in names:
            return key
    return "other"


if __name__ == "__main__":

    eyp = get_eyp_ads()
    sim_occs = get_sim_occ_ads()
    all_job_ads = pd.concat([eyp, sim_occs], axis=0).drop_duplicates()

    all_job_ads = all_job_ads[
        ~all_job_ads["itl_1_name"].isin(["Northern Ireland", "Scotland", "Wales"])
    ]  # Excluding adjacent nations for now

    all_job_ads["geo"] = all_job_ads["itl_1_name"].apply(
        map_groups, args=(GEO_MAPPING,)
    )
    all_job_ads["sector_group"] = all_job_ads["sector"].apply(
        map_groups, args=(SECTOR_MAPPING,)
    )

    all_job_ads["year"] = pd.to_datetime(all_job_ads["created"]).dt.year
    # Ideally we would stratify by ITL 1 as well but the minimum sample size ends up too small
    all_job_ads["stratify_col"] = (
        all_job_ads["geo"].astype(str) + "_" + all_job_ads["sector_group"]
    )

    all_job_ads = all_job_ads[all_job_ads["itl_1_code"].notnull()]
    all_job_ads = all_job_ads[all_job_ads["year"] > 2020]
    logging.info(f"Total number of job ads (post 2020): {len(all_job_ads)}")

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
        f"job_quality/early_years/evaluation_sample/job_ads_by_sector_region_sample_size_{sample_n_rows}.parquet",
    )
