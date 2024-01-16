"""
Getters to retrieve OJO data from the database.
"""
from dap_job_quality import PRINZ_BUCKET_NAME
from dap_job_quality.getters.data_getters import load_s3_data

import os
from typing import Dict, List
import pandas as pd


# Let's currently get a sample of the data from PRINZ to work with
# for labelling etc.
def get_ojo_sample() -> pd.DataFrame:
    """Gets ojo sample data (100,000 job ads) from s3.

    Returns:
        pd.Dataframe: ojo sample data with the fields:
            - `id`: unique identifier for the job ad
            - `job_title_raw`
            - `created`: datetime when the job was listed?
            - `description`: job description
            - `itl_3_code`
            - `itl_3_name`
    """
    return load_s3_data(
        PRINZ_BUCKET_NAME,
        "outputs/data/ojo_application/deduplicated_sample/ojo_sample.csv",
    )


def get_ojo_job_title_sample() -> pd.DataFrame:
    """Gets ojo sample data with job title and sectors information from s3

    Returns:
        pd.Dataframe: ojo sample data with the following fields:
            - id: unique identifier for the job ad
            - company_raw: company name
            - job_title_raw
            - job_location_raw
            - created: datetime when the job was listed?
            - type: who listed the ad (e.g. "Recruitement consultancy", "Direct employer")
            - sector
            - parent_sector
            - knowledge_domain: eg "Engineering", "Legal"
            - occupation: standardised job title eg "Manager consultant"
    """
    return load_s3_data(
        PRINZ_BUCKET_NAME,
        "outputs/data/ojo_application/deduplicated_sample/job_title_data_sample.csv",
    )


def get_ojo_location_sample() -> pd.DataFrame:
    """Gets ojo sample data with location information from s3

    Returns:
        pd.Dataframe: ojo sample data with the fields:
            - id: unique identifier of the job ad
            - job_location_raw
            - itl_1_code: eg "TLJ"
            - itl_1_name: eg "South East (England)
            - itl_2_code
            - itl_2_name
            - itl_3_code
            - itl_3_name
            - is_uk: 1.0 if true, 0.0 if false
            - is_large_geo: 1.0 if true, 0.0 if false (what does this mean?)
            - location: standardised from job_location_raw
            - coordinates
    """
    return load_s3_data(
        PRINZ_BUCKET_NAME,
        "outputs/data/ojo_application/deduplicated_sample/locations_data_sample.csv",
    )


def get_ojo_salaries_sample() -> pd.DataFrame:
    """Gets ojo sample data with salaries information from s3

    Returns:
        pd.Dataframe: ojo sample data with the fields:
            - id: unique identifier of the job ad
            - min_annualised_salary
            - max_annualised_salary
    """
    return load_s3_data(
        PRINZ_BUCKET_NAME,
        "outputs/data/ojo_application/deduplicated_sample/salaries_data_sample.csv",
    )


def get_ojo_skills_sample() -> pd.DataFrame:
    """Gets ojo sample data with skills information from s3

    Returns:
        pd.Dataframe: ojo sample data
    """
    return load_s3_data(
        PRINZ_BUCKET_NAME,
        "outputs/data/ojo_application/deduplicated_sample/skills_data_sample.csv",
    )
