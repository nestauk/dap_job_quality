import pandas as pd

from dap_job_quality.getters.data_getters import load_s3_data
from dap_job_quality import BUCKET_NAME


def get_eyp_ads():
    return load_s3_data(BUCKET_NAME, "job_quality/early_years/eyp_job_ads.parquet")


def get_sim_occ_ads():
    return load_s3_data(BUCKET_NAME, "job_quality/early_years/sim_occs_job_ads.parquet")


def get_jq_sentences():
    return load_s3_data(
        BUCKET_NAME, "job_quality/early_years/jq_sentences/jq_sentences_df_2023.parquet"
    )


def get_ngrams_and_matches() -> pd.DataFrame:
    """
    Unique ngrams that occurred in a sample of 27,772 sentences that were identified as being
    about job quality. These were taken from a sample of 83,150 job ads from 2023,
    covering EYP and similar occupations.

    Returns:
        pd.DataFrame: Dataframe with the columns ['ngrams', 'target_phrase', 'cosine_similarity']
    """
    return load_s3_data(
        BUCKET_NAME,
        "job_quality/early_years/jq_sentences/unique_ngrams_2023_matched_2024-06-19.parquet",
    )


def get_jq_sentences_and_matches() -> pd.DataFrame:
    return load_s3_data(
        BUCKET_NAME,
        "job_quality/early_years/jq_sentences/jq_sentences_2023_matched_2024-06-19.parquet",
    )


def get_stratified_sample() -> pd.DataFrame:
    return load_s3_data(
        BUCKET_NAME,
        "job_quality/early_years/evaluation_sample/job_ads_sample_5000.parquet",
    )
