import pandas as pd

from dap_job_quality.getters.data_getters import save_to_s3
from dap_job_quality import BUCKET_NAME
from dap_job_quality.utils.text_cleaning import clean_text

if __name__ == "__main__":

    # Load data
    ojo_descriptions = pd.read_parquet(
        "s3://open-jobs-lake/latest_output_tables/descriptions_fixed.parquet"
    )
    eyp_jobs = pd.read_parquet(
        "s3://afs-early-years-labour-market-analysis/inputs/2024/ojd_daps_extract/enriched_relevant_job_adverts_eyp.parquet"
    )
    sim_jobs = pd.read_parquet(
        "s3://afs-early-years-labour-market-analysis/inputs/2024/ojd_daps_extract/enriched_relevant_job_adverts_sim_occs.parquet"
    )

    # To make the join quicker
    all_sample_ids = set(eyp_jobs["id"].tolist() + sim_jobs["id"].to_list())
    ojo_descriptions_sample = ojo_descriptions[
        ojo_descriptions["id"].isin(all_sample_ids)
    ]

    # Add the descriptions
    eyp_jobs_desc = eyp_jobs.merge(ojo_descriptions_sample, on="id", how="left")
    sim_jobs_desc = sim_jobs.merge(ojo_descriptions_sample, on="id", how="left")

    # Clean the descriptions
    eyp_jobs_desc = eyp_jobs_desc.assign(
        clean_description=lambda x: x.description.apply(clean_text)
    ).drop(columns=["description"])

    sim_jobs_desc = sim_jobs_desc.assign(
        clean_description=lambda x: x.description.apply(clean_text)
    ).drop(columns=["description"])

    # Save
    save_to_s3(
        BUCKET_NAME, eyp_jobs_desc, "job_quality/early_years/eyp_job_ads.parquet"
    )
    save_to_s3(
        BUCKET_NAME, sim_jobs_desc, "job_quality/early_years/sim_occs_job_ads.parquet"
    )
