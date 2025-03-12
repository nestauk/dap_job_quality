import ast
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
import os
import pandas as pd
import pathlib
import re

from dap_job_quality.getters.data_getters import save_to_s3
from dap_job_quality import BUCKET_NAME, PROJECT_DIR, logger


def get_soc_codes_of_interest(
    excel_file="extendedsoc2020structureanddescriptionsexcel161020241.xlsx",
    sheet_name="Extended SOC framework",
    highlight_colours=["FFFFFF00", "FF00FF00"],
):
    # Load the workbook and select the sheet
    wb = load_workbook(excel_file, data_only=True)
    sheet = wb[sheet_name]

    # Step 1: Get the column names from the second row
    column_names = [cell.value for cell in sheet[2]]  # Second row contains headers

    # Specify the target column name
    target_column_name = "Sub-Unit Group"

    # Check if the target column name exists
    if target_column_name not in column_names:
        raise ValueError(f"Column '{target_column_name}' not found in the sheet.")

    # Find the column index (1-based indexing for Excel)
    column_index = column_names.index(target_column_name) + 1

    # Extract yellow-highlighted values from the target column
    highlighted_codes = []
    all_highlights = []
    for row in sheet.iter_rows(
        min_row=3, min_col=column_index, max_col=column_index
    ):  # Start from the third row (data rows)
        cell = row[0]
        fill = cell.fill
        if fill.fgColor.rgb not in all_highlights:
            all_highlights.append(fill.fgColor.rgb)
        if fill and fill.fgColor and fill.fgColor.rgb in highlight_colours:
            highlighted_codes.append(cell.value)

    logger.info(f"Distinct cell colours: {all_highlights}")
    # Ensure the list is unique and drop None values
    highlighted_codes = [v for v in highlighted_codes if v is not None]
    highlighted_codes = list(set(highlighted_codes))

    data = []
    for row in sheet.iter_rows(
        min_row=3, values_only=True
    ):  # Start from the third row (data rows)
        data.append(row)

    soc_codes_df = pd.DataFrame(data, columns=column_names)
    soc_codes_df_filtered = soc_codes_df[
        soc_codes_df["Sub-Unit Group"].isin(highlighted_codes)
    ][["Sub-Unit Group", "Group Title"]]
    wb.close()

    return highlighted_codes, soc_codes_df_filtered


def replace_nan_with_none(text: str):
    """
    Replaces occurrences of 'nan' with 'None' only when 'nan' has word boundaries on both sides.
    """
    return re.sub(r"\bnan\b", "None", text)


if __name__ == "__main__":
    all_soc = pd.read_parquet(
        "s3://prinz-green-jobs/outputs/data/ojo_application/extracted_green_measures/20241118/ojo_all_occupation_green_measures_production_True.parquet"
    )
    job_titles = pd.read_parquet(
        "s3://prinz-green-jobs/outputs/data/ojo_application/deduplicated_sample/20241114/latest_update_20241114_titles.parquet"
    )

    logger.info("Merging job titles with SOC info...")
    job_titles_soc = pd.merge(
        job_titles, all_soc, left_on="id", right_on="job_id", how="inner"
    )

    job_titles_soc["SOC"] = job_titles_soc["SOC"].apply(replace_nan_with_none)
    job_titles_soc["SOC"] = job_titles_soc["SOC"].apply(ast.literal_eval)

    logger.info("Extracting 2020 4 digit SOC codes...")
    job_titles_soc["soc_2020_4_digit"] = job_titles_soc["SOC"].apply(
        lambda x: x["SOC_2020"] if x is not None else None
    )
    job_titles_soc["soc_2020_4_digit_ext"] = job_titles_soc["SOC"].apply(
        lambda x: x["SOC_2020_EXT"] if x is not None else None
    )

    logger.info("Filtering job titles with health SOC codes...")
    highlighted_codes, soc_codes_df_filtered = get_soc_codes_of_interest(
        excel_file=PROJECT_DIR
        / "dap_job_quality/analysis/healthcare_analysis/extendedsoc2020structureanddescriptionsexcel161020241.xlsx",
        sheet_name="Extended SOC framework",
    )

    health_jobs = pd.merge(
        job_titles_soc,
        soc_codes_df_filtered,
        left_on="soc_2020_4_digit_ext",
        right_on="Sub-Unit Group",
        how="inner",
    )

    in_scope_ids = health_jobs["id"]
    logger.info(f"{len(in_scope_ids)} job adverts found with health SOC codes.")

    save_to_s3(
        BUCKET_NAME, in_scope_ids, "job_quality/health_social_care/health_jobs_ids.pkl"
    )

    save_to_s3(
        BUCKET_NAME,
        health_jobs,
        "job_quality/health_social_care/health_jobs_titles_w_soc_codes.parquet",
    )
