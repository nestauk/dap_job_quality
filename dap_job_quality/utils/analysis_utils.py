import numpy as np
import pandas as pd
import re


def create_wide_table(df):
    # Create a table with one row per ID, and a boolean column for each subcategory
    dimensions_wide = df[["id", "subcategory"]].drop_duplicates()
    dimensions_wide["value"] = 1
    dimensions_wide = dimensions_wide.pivot(index="id", columns="subcategory").fillna(0)
    dimensions_wide.columns = dimensions_wide.columns.get_level_values(1)
    return dimensions_wide.reset_index()


def calculate_hourly_wage(
    row, salary_unit_col="raw_salary_unit", hours_per_day=7.5, hours_per_year=37.5 * 52
):
    """Infer hourly pay.

    If the pay is given per hour, accept this amount. Otherwise if it is given per day, divide by hours per day,
    and if it is given per year, divide by the likely number of hours in a year.

    Args:
        row (_type_): _description_
        hours_per_day (float, optional): _description_. Defaults to 7.5.
        hours_per_year (_type_, optional): _description_. Defaults to 37.5*52.

    Returns:
        _type_: _description_
    """
    if row[salary_unit_col] == "hour":
        return row["raw_salary_float"]
    elif row[salary_unit_col] == "day":
        return row["raw_salary_float"] / hours_per_day
    elif row[salary_unit_col] == "year":
        return row["raw_salary_float"] / hours_per_year
    else:
        return np.nan


def extract_salary_info(text: str):
    # Regular expression to match the salary pattern
    salary_pattern = re.compile(
        r"£\s*(\d+(?:,\d{3})*(?:\.\d{2})?)\s*(?:-|to)?\s*£?\s*(\d+(?:,\d{3})*(?:\.\d{2})?)?\s*(per\s*(hour|day|annum|year))?",
        re.IGNORECASE,
    )

    matches = salary_pattern.findall(text)
    salary_info = []

    for match in matches:
        min_salary = float(match[0].replace(",", ""))
        max_salary = float(match[1].replace(",", "")) if match[1] else min_salary
        rate = match[3].lower() if match[3] else np.nan

        salary_info.append(
            {"min_salary": min_salary, "max_salary": max_salary, "rate": rate}
        )

    return salary_info


# # Example usage
# text = """
# Pay £120.00 - £140.00 per day
# What we Offer: £13.00 - £15.00 per hour inclusive of service charge
# £21,650 base
# £24,950 OTE
# Salary: £21,500 (FTE equivalent)
# £110 - £120 per day
# £9.60 per hour
# £21,000-22,000 per annum
# £14.00 - £16.00 per hour
# £65 to £95 per day
# """

# test_salaries = extract_salary_info(text)
# for sal in test_salaries:
#     print(sal)

# Define the keywords for permanent and temporary contracts
permanent_keywords = r"\b(permanent|ongoing|long-term|long term|indefinite)\b"
temporary_keywords = r"(contract will last from|temporary|fixed-term|fixed term|maternity|short-term|interim|seasonal|short and long term positions|short term positions|month contract|on a contract basis|contract position|contract period)"
apprenticeship_keywords = r"apprentice"


def classify_contract_type(sentence):
    """
    Classifies the contract type based on keywords in the sentence.
    Returns 'Permanent' if permanent keywords are found,
    'Temporary' if temporary keywords are found, otherwise 'Unknown'.

    Search for 'Temporary' first because many jobs are temporary 'with the potential to become permanent'.
    """
    if re.search(temporary_keywords, sentence, re.IGNORECASE):
        return "Temporary"
    elif re.search(apprenticeship_keywords, sentence, re.IGNORECASE):
        return "Apprenticeship"
    elif re.search(permanent_keywords, sentence, re.IGNORECASE):
        return "Permanent"
    else:
        return "Unknown"
