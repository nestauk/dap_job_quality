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
            - id: unique identifier for the job ad
            - job_title_raw: job title as listed in the dataset
            - created: datetime when the job was listed
            - description: job description
            - itl_3_code: ITL 3 code for the location of the job
            - itl_3_name: ITL 3 name for the location of the job
    """
    return load_s3_data(
        PRINZ_BUCKET_NAME,
        "outputs/data/ojo_application/deduplicated_sample/ojo_sample.csv",
    )


def get_ojo_job_title_sample() -> pd.DataFrame:
    """Gets ojo sample data (100,000 job ads) with
        job title and sectors information from s3.

    Returns:
        pd.Dataframe: ojo sample data with the following fields:
            - id: unique identifier for the job ad
            - company_raw: company name
            - job_title_raw: job title as originally listed
            - job_location_raw: location of the job as originally listed
            - created: datetime when the job was listed in the dataset
            - type: who listed the ad (e.g. "Recruitement consultancy", "Direct employer")
            - sector: dataset specific sector eg "Accountancy (Qualified)"
            - parent_sector: dataset specific parent sector eg "Accountancy"
            - knowledge_domain: eg "Engineering", "Legal"
            - occupation: standardised job title eg "Manager consultant"
    """
    return load_s3_data(
        PRINZ_BUCKET_NAME,
        "outputs/data/ojo_application/deduplicated_sample/job_title_data_sample.csv",
    )


def get_ojo_location_sample() -> pd.DataFrame:
    """Gets ojo sample data (100,000 job ads) with
        processed location information from s3.

    Returns:
        pd.Dataframe: ojo sample data with the fields:
            - id: unique identifier of the job ad
            - job_location_raw: location of the job as originally listed
            - itl_1_code: eg "TLJ"
            - itl_1_name: eg "South East (England)
            - itl_2_code: eg "TLJ1"
            - itl_2_name: eg "Berkshire, Buckinghamshire and Oxfordshire"
            - itl_3_code: eg "TLJ11"
            - itl_3_name: eg "Buckinghamshire"
            - is_uk: 1.0 if true, 0.0 if false
            - is_large_geo: 1.0 if true, 0.0 if false
            - location: standardised from job_location_raw
            - coordinates: coordinates of the location
    """
    return load_s3_data(
        PRINZ_BUCKET_NAME,
        "outputs/data/ojo_application/deduplicated_sample/locations_data_sample.csv",
    )


def get_ojo_salaries_sample() -> pd.DataFrame:
    """Gets ojo sample data (100,000 job ads) with
        processed salaries information from s3.

    Returns:
        pd.Dataframe: ojo sample data with the fields:
            - id: unique identifier of the job ad
            - min_annualised_salary: minimum annualised salary
            - max_annualised_salary: maximum annualised salary
    """
    return load_s3_data(
        PRINZ_BUCKET_NAME,
        "outputs/data/ojo_application/deduplicated_sample/salaries_data_sample.csv",
    )


def get_ojo_skills_sample() -> pd.DataFrame:
    """Gets ojo sample data (100,000 job ads) with
        skills information from s3.

    Returns:
        pd.Dataframe: ojo sample data with the fields:
            - id: unique identifier for the job ad from which the skill was extracted
            - skill_label: the skill as extracted by our skills extractor
                eg "Advanced IT skills"
            - esco_label: the skill from the ESCO taxonomy that the skill
                maps to eg "using digital tools for collaboration and productivity"
            - esco_id: identifier for the ESCO skill in the ESCO taxonomy
                eg S5.6.1
    """
    return load_s3_data(
        PRINZ_BUCKET_NAME,
        "outputs/data/ojo_application/deduplicated_sample/skills_data_sample.csv",
    )
