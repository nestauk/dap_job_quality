"""This script merges the fields required from different OJO tables for the salary analysis undertaken in salary_external_tests.py.

The final table is saved in 'job_quality/salary_analysis/salary_analysis_df.csv'

A warning that the data download takes some time, so will likely take ~30 mins to complete.

To run this file, run from the project root folder:

python -m dap_job_quality.utils.create_df_for_salary_analysis

    """

from dap_job_quality import BUCKET_NAME, PRINZ_BUCKET_NAME, logger
from dap_job_quality.getters.data_getters import load_s3_data, load_s3_excel, save_to_s3
import pandas as pd


# Current OJO data paths
OCC_MEASURES_S3 = "outputs/data/ojo_application/extracted_green_measures/20240213/ojo_large_sample_occupation_green_measures_production_true.csv"
LARGE_OJO_SAMPLE_S3 = (
    "outputs/data/ojo_application/deduplicated_sample/all_ojo_sample.parquet"
)
LOCATION_MEASURES_S3 = (
    "outputs/data/ojo_application/deduplicated_sample/all_locations_data_sample.parquet"
)
SALARY_MEASURES_S3 = (
    "outputs/data/ojo_application/deduplicated_sample/all_salaries_data_sample.parquet"
)


def merge_ojo_df(
    ojo_df: pd.DataFrame,
    ojo_occ: pd.DataFrame,
    ojo_loc: pd.DataFrame,
    ojo_sal: pd.DataFrame,
    save_file=False,
) -> pd.DataFrame:
    """Takes de-duplicated OJO data and returns a dataframe with the relevant data
    (ITL1 codes, SOC2 and SOC4 codes, salary min, salary max, salary mean, and job title)
    """

    # Merge dataframes
    ojo_df = ojo_df.merge(
        ojo_loc[["id", "itl_1_code", "itl_1_name"]], on="id", how="left"
    )  # Get ITL1 code and name
    ojo_df = ojo_df.merge(
        ojo_occ[["job_id", "SOC"]], left_on="id", right_on="job_id", how="left"
    ).drop(
        columns="job_id"
    )  # Get raw SOC level 4 mapping
    ojo_df = ojo_df.merge(
        ojo_sal, on="id", how="left"
    )  # Min and max annualised salaries

    # Get mean salaries
    ojo_df["mean_salary"] = ojo_df[
        ["min_annualised_salary", "max_annualised_salary"]
    ].mean(axis=1)

    # Get SOC 4 digit codes in single column
    ojo_df["SOC_clean"] = ojo_df["SOC"].fillna("None")
    ojo_df["SOC_clean"] = ojo_df["SOC_clean"].apply(lambda x: x.replace("nan", "None"))
    ojo_df["SOC_clean"] = ojo_df["SOC_clean"].apply(
        lambda x: eval(x) if isinstance(x, str) else x
    )
    ojo_df["SOC_4digit"] = ojo_df["SOC_clean"].apply(
        lambda x: x.get("SOC_2020") if isinstance(x, dict) else x
    )
    ojo_df["SOC_2digit"] = ojo_df["SOC_4digit"].apply(
        lambda x: x[0:2] if isinstance(x, str) else x
    )

    # Check all values have carried over from the SOC extraction
    soc_columns = [col for col in ojo_df.columns if "SOC" in col]

    for col in soc_columns:
        isnull_counts = ojo_df[soc_columns].isnull().sum()
        all_counts_same = isnull_counts.nunique() == 1

    if all_counts_same:
        logger.info("SOC successfully extracted - no additional missing values")
    else:
        logger.info("Some additional missing values - check if SOC extracted correctly")

    if save_file == True:
        save_to_s3(
            BUCKET_NAME, ojo_df, "job_quality/salary_analysis/salary_analysis_df.csv"
        )
        logger.info(
            f"File saved to {BUCKET_NAME}/job_quality/salary_analysis/salary_analysis_df.csv"
        )

    return ojo_df


if __name__ == "__main__":
    ojo_occ = load_s3_data(PRINZ_BUCKET_NAME, OCC_MEASURES_S3)
    ojo_loc = load_s3_data(PRINZ_BUCKET_NAME, LOCATION_MEASURES_S3)
    ojo_sal = load_s3_data(PRINZ_BUCKET_NAME, SALARY_MEASURES_S3)
    ojo_df = load_s3_data(PRINZ_BUCKET_NAME, LARGE_OJO_SAMPLE_S3)
    ojo_df.drop(columns=["description"], inplace=True)

    clean_df = merge_ojo_df(ojo_df, ojo_occ, ojo_loc, ojo_sal, save_file=True)
