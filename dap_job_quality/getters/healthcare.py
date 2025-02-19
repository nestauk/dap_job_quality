import pandas as pd

from dap_job_quality import BUCKET_NAME
from dap_job_quality.utils import analysis_utils
from dap_job_quality.getters.data_getters import load_s3_json


def get_ids():
    return pd.read_pickle(
        "s3://open-jobs-lake/job_quality/health_social_care/health_jobs_ids.pkl"
    )


def get_locations():
    in_scope_ids = get_ids()

    locations = pd.read_parquet(
        "s3://prinz-green-jobs/outputs/data/ojo_application/deduplicated_sample/20241114/latest_update_20241114_locations.parquet"
    )
    locations_filtered = locations[locations["id"].isin(in_scope_ids)]
    return locations_filtered


def get_salaries():
    salaries = pd.read_parquet(
        "s3://open-jobs-lake/job_quality/health_social_care/salaries/salary_metadata_production_True.parquet"
    )
    salaries_deduplicated = salaries.drop_duplicates(subset=["id"])
    salaries_complete = salaries_deduplicated[
        salaries_deduplicated["raw_salary_unit"].notna()
    ]
    salaries_complete["raw_salary_unit"] = salaries_complete[
        "raw_salary_unit"
    ].str.lower()

    salaries_complete["raw_salary_float"] = salaries_complete[
        "raw_salary_float"
    ].fillna(salaries_complete["raw_min_salary_float"])

    salaries_complete["hourly_wage"] = salaries_complete.apply(
        analysis_utils.calculate_hourly_wage, axis=1
    )

    return salaries_complete


def get_occupations():
    soc_mapping = load_s3_json(
        "prinz-green-jobs",
        "outputs/data/ojo_application/extracted_green_measures/20241118/soc_name_dict.json",
    )

    job_titles = pd.read_parquet(
        "s3://open-jobs-lake/job_quality/health_social_care/health_jobs_titles_w_soc_codes.parquet"
    )

    job_titles["soc_4_digit_name"] = job_titles["soc_2020_4_digit"].map(
        soc_mapping["soc_2020_4"]
    )

    return job_titles


def get_locations_soc_codes():
    locations = get_locations()
    job_titles = get_occupations()

    locations_w_soc = pd.merge(locations, job_titles, on="id", how="left")

    return locations_w_soc


def get_salaries_w_soc_region():
    salaries = get_salaries()
    locations_w_soc = get_locations_soc_codes()

    # left join this way because the salary data will be smaller (various missing salaries)
    salaries_w_soc_region = pd.merge(salaries, locations_w_soc, on="id", how="left")
    return salaries_w_soc_region


def get_contracts_full():
    """Long dataframe where each row is one sentence, and each sentence has been matched to a contract type."""
    return pd.read_parquet(
        "s3://open-jobs-lake/job_quality/health_social_care/health_jobs_jq_contract.parquet"
    )


def get_contract_per_job():
    """Returns a dataframe containing unique job IDs and *one* contract type per job.

    NB not all jobs were identified as containing contract info (via the job quality pipeline) so this is a subset.
    """
    contracts = get_contracts_full()

    # counts = contracts.groupby(['id', 'classified_contract_type']).size().reset_index(name='count')

    # # Find the most frequent 'classified_contract_type' for each job
    # most_frequent = counts.loc[counts.groupby('id')['count'].idxmax()]

    # gather all the different contract types that were applied to the same job
    contract_df = (
        contracts.groupby("id")["classified_contract_type"].agg(list).reset_index()
    )
    contract_df["n_contract_types"] = contract_df["classified_contract_type"].apply(len)

    contract_df["final_contract_type"] = contract_df["classified_contract_type"].apply(
        analysis_utils.determine_contract_type
    )

    return contract_df


def get_contracts_complete():
    contract_df = get_contract_per_job()

    return contract_df[contract_df["final_contract_type"] != "Unknown"]
