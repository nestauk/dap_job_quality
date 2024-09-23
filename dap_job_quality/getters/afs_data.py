import numpy as np
import pandas as pd
import torch

from dap_job_quality.getters.data_getters import load_s3_data, download_from_s3
from dap_job_quality import BUCKET_NAME, PROJECT_DIR


def get_eyp_ads():
    """Get a sample of ads for Early Years Professionals."""
    return load_s3_data(BUCKET_NAME, "job_quality/early_years/eyp_job_ads.parquet")


def get_sim_occ_ads():
    """Get a sample of ads for similar occupations to Early Years Professionals.
    This includes hospitality and retail.
    """
    return load_s3_data(BUCKET_NAME, "job_quality/early_years/sim_occs_job_ads.parquet")


def get_jq_sentences():
    """Created by the script dap_job_quality/pipeline/eyp/ngram_analysis/extract_jq_sentences.py"""
    return load_s3_data(
        BUCKET_NAME, "job_quality/early_years/jq_sentences/jq_sentences_df_2023.parquet"
    )


def ngram_analysis_embeddings():
    """
    Created in the script dap_job_quality/pipeline/eyp/ngram_analysis/get_embeddings.py
    """
    file_name = PROJECT_DIR / "inputs/eyp/embeddings_2023.npy"
    download_from_s3(
        BUCKET_NAME, "job_quality/early_years/embeddings_2023.npy", file_name
    )

    np_array = np.load(file_name)
    tensor = torch.from_numpy(np_array)
    return tensor


def ngram_analysis_identifiers():
    """
    Created in the script dap_job_quality/pipeline/eyp/ngram_analysis/get_embeddings.py
    """
    file_name = PROJECT_DIR / "inputs/eyp/identifiers_2023.npy"
    download_from_s3(
        BUCKET_NAME, "job_quality/early_years/identifiers_2023.npy", file_name
    )

    return np.load(file_name)


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
        "job_quality/early_years/evaluation_sample/job_ads_by_sector_region_sample_size_19896.parquet",
    )


def get_enhanced_salary_data():
    """
    Produced by `pipeline/eyp/eyp_auxiliary_data.py`. That script takes in the raw data, filters it to IDs that
    are in "job_quality/early_years/evaluation_sample/job_ads_by_sector_region_sample_size_19896.parquet", and retrieves the raw salary info
    for those records.

    Returns:
        pd.DataFrame: _description_
    """
    return pd.read_parquet(
        "s3://open-jobs-lake/job_quality/early_years/evaluation_sample/job_ads_by_sector_region_sample_size_19896_metadata_production_True.parquet"
    )
