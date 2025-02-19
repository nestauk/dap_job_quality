from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import re
from typing import List, Dict, Union, Optional, Set

from dap_job_quality.getters.keywords import get_keywords


def create_wide_table(df):
    # Create a table with one row per ID, and a boolean column for each subcategory
    dimensions_wide = df[["id", "subcategory"]].drop_duplicates()
    dimensions_wide["value"] = 1
    dimensions_wide = dimensions_wide.pivot(index="id", columns="subcategory").fillna(0)
    dimensions_wide.columns = dimensions_wide.columns.get_level_values(1)
    return dimensions_wide.reset_index()


### HOURLY PAY ###


def calculate_hourly_wage(
    row: pd.Series,
    salary_unit_col: str = "raw_salary_unit",
    hours_per_day: float = 7.5,
    hours_per_year: float = 37.5 * 52,
) -> float:
    """Infer hourly pay.

    If the pay is given per hour, accept this amount. Otherwise if it is given per day, divide by hours per day,
    and if it is given per year, divide by the likely number of hours in a year.

    Args:
        row (pd.Series): A row in a dataframe.
        salary_unit_col (str): The name of the column that contains the salary unit (eg 'hour', 'day', 'year').
        hours_per_day (float, optional): The number of working hours in a day. Defaults to 7.5.
        hours_per_year (float, optional): The number of working hours in a year. Defaults to 37.5*52.

    Returns:
        float: The calculated hourly wage or NaN if this cannot be calculated.
    """
    if row[salary_unit_col] == "hour":
        return row["raw_salary_float"]
    elif row[salary_unit_col] == "day":
        return row["raw_salary_float"] / hours_per_day
    elif row[salary_unit_col] == "year":
        return row["raw_salary_float"] / hours_per_year
    else:
        return np.nan


def extract_salary_info(text: str) -> List[Dict[str, Union[float, str, np.float64]]]:
    """
    Extracts salary information from the given text using regular expressions.

    Regex to look for:
    - minimum salary
    - maximum salary (if provided)
    - rate (e.g., 'hour', 'day', 'year')

    The function looks for salary patterns like "£30,000 - £40,000 per year" or "£15 per hour"
    and returns a list of dictionaries containing minimum salary, maximum salary, and the rate
    (e.g., 'hour', 'day', 'year'). If the maximum salary is not provided, the minimum salary is used for both.

    Args:
        text (str): The text containing salary information.

    Returns:
        List[Dict[str, Union[float, str, np.float64]]]: A list of dictionaries, each containing:
            - 'min_salary' (float): The minimum salary.
            - 'max_salary' (float): The maximum salary.
            - 'rate' (str or np.float64): The rate (e.g., 'hour', 'day', 'year'). If not provided, returns NaN.
    """

    salary_pattern = re.compile(
        r"£\s*(\d+(?:,\d{3})*(?:\.\d{2})?)\s*(?:-|to)?\s*£?\s*(\d+(?:,\d{3})*(?:\.\d{2})?)?\s*(per\s*(hour|day|annum|year))?",
        re.IGNORECASE,
    )

    matches = salary_pattern.findall(text)
    salary_info = []

    for match in matches:
        min_salary = float(match[0].replace(",", ""))
        # If the second group is empty, set the same value for both max and min
        max_salary = float(match[1].replace(",", "")) if match[1] else min_salary
        # the second group is (per (day/hour/year)) so group 3 is the actual rate
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

### CONTRACT ###

# Define the keywords for permanent and temporary contracts
permanent_keywords = r"\b(permanent|ongoing|long-term|long term|indefinite)\b"
temporary_keywords = r"(contract will last from|contract until|temporary|fixed-term|fixed term|maternity|short-term|interim|seasonal|short and long term|short term|long and short term|month[s]? contract|on a contract basis|contract position|contract period|\d{1,2}[- ]?month placement|rolling contract|short[- ]?term contract|[cC]ontract\s+\d{1,2}\s+month|initial \d{1,2}[- ]?month|\d{1,2}[-\s+]?month\s+initial|contract length|block booking|contract\s+duration|duration\s+\d{1,2}\s+month|secondment)"
apprenticeship_keywords = r"apprentice"
zero_hours_keywords = r"\b(zero hour|zero-hour|zero hours|zero-hours|on call|casual contract|casual cover|bank staff|staff bank|bank work|bank basis)\b"
zero_hours_exclusion = r"\b(no zero hours|no zero-hours)\b"


def classify_contract_type(sentence: str):
    """
    Classifies the contract type based on keywords in the sentence.
    Returns 'Permanent' if permanent keywords are found,
    'Temporary' if temporary keywords are found, 'Zero Hours' if zero hours contract keywords are found, otherwise 'Unknown'.

    Search for 'Temporary' first because many jobs are temporary 'with the potential to become permanent'.
    """
    if re.search(temporary_keywords, sentence, re.IGNORECASE):
        return "Temporary"
    elif re.search(apprenticeship_keywords, sentence, re.IGNORECASE):
        return "Apprenticeship"
    elif re.search(permanent_keywords, sentence, re.IGNORECASE):
        return "Permanent"
    elif re.search(zero_hours_exclusion, sentence, re.IGNORECASE):
        return "Unknown"  # Exclude cases like "no zero hours"
    elif re.search(zero_hours_keywords, sentence, re.IGNORECASE):
        return "Zero Hours"
    else:
        return "Unknown"


### HOURS ###


def match_hours_per_week(text: str) -> List[Dict[str, int]]:
    """Extract hours per week from free text.

    Looks for substrings that contain a number followed by 'hours', 'hrs', 'h', or 'hour' and
    optionally followed by 'per week' or 'p/w'. Extracts the minimum and maximum hours if a range is provided.

    Args:
        text (str): Free text of a job description.

    Returns:
        List[Dict[str, int]]: List of dicts where each dict is a min and max hours.
    """

    working_hours = []

    hours_pattern = re.compile(
        r"(\d{1,2}(?:-\d{1,2})?)\s*(hours|hrs|h|hour)\s*(per\s*week|p\/w|a\s*week)?",
        re.IGNORECASE,
    )

    explicit_hours = hours_pattern.findall(text)

    if explicit_hours:
        for match in explicit_hours:
            hours_range = match[0]
            if "-" in hours_range:
                min_hours, max_hours = map(int, hours_range.split("-"))
            else:
                min_hours = max_hours = int(hours_range)
            working_hours.append({"min_hours": min_hours, "max_hours": max_hours})

    return working_hours


# # Example usage
# hours_texts = ['40 hours per week',
# 'Full time Hours - 8:30am - 4:30pm',
# 'Working days : Tuesday 10am-2pm',
# '35 hours per week',
# 'Full-time contract of employment, 39 hours per week',
# '40-45 hours p/w',
# 'Full-time',
# '4 Days a Week',
# '4 days per week Monday – Thursday, 8.30AM – 3.30PM',
# 'part-time 24 hour per week contract',
# 'Working hours: 40 hours over 5 out of 7 days']

# [match_hours_per_week(text) for text in hours_texts]


def parse_time(time_str: str) -> Optional[datetime]:
    """Parses a time string into a datetime object.

    Args:
        time_str (str): The string to be parsed.

    Returns:
        Optional[datetime]: The parsed datetime object or None if the parsing fails.
    """

    time_formats = [
        "%I:%M%p",  # e.g., "8:30am"
        "%I.%M%p",  # e.g., "8.30am"
        "%I:%M %p",  # e.g., "8:30 am"
        "%I.%M %p",  # e.g., "8.30 am"
        "%I%p",  # e.g., "8am"
        "%I %p",  # e.g., "8 am"
        "%I:%M",  # e.g., "8:30"
        "%I.%M",  # e.g., "8.30"
        "%I",  # e.g., "8"
    ]

    for time_format in time_formats:
        try:
            return datetime.strptime(time_str, time_format)
        except ValueError:
            continue
    return None


def extract_time_difference(text: str) -> Optional[float]:
    """
    Extracts and calculates the time difference (in hours) from a given text containing time ranges.

    This function uses regular expressions to find time ranges (e.g., "9:00 am - 5:00 pm" or "14:30 - 16:00")
    and returns the difference in hours between the start and end times. If the end time is earlier than the
    start time, it assumes the time range spans to the next day.

    The main regex searches for:
    - Start time: 1-2 digits followed by a colon or period, then 2 digits, and optionally followed by 'am' or 'pm'.
    - A dash '-' separator
    - End time: Same format as the start time.

    Args:
        text (str): The text containing the time range.

    Returns:
        Optional[float]: The time difference in hours as a float. If no valid time range is found, returns None.
    """

    # Regex pattern to capture time ranges
    time_range_pattern = re.compile(
        r"(\d{1,2}[:\.]\d{2}|\d{1,2})(?:\s*[apAP][mM])?\s*-\s*(\d{1,2}[:\.]\d{2}|\d{1,2})(?:\s*[apAP][mM])?",
        re.IGNORECASE,
    )

    matches = time_range_pattern.findall(text)
    if not matches:
        return None

    for start_time_str, end_time_str in matches:
        # Clean up and parse the times
        start_time = parse_time(start_time_str.strip())
        end_time = parse_time(end_time_str.strip())

        if not start_time or not end_time:
            continue

        # If end time is earlier than start time, assume it spans to the next day (24-hour format)
        if end_time <= start_time:
            end_time += timedelta(hours=12)

        # Calculate the time difference in hours
        time_difference = (end_time - start_time).seconds / 3600.0
        return time_difference

    return None


# # Example usage
# texts = [
#     "8.30-11.30am",
#     "8.30am - 12.30pm",
#     "8.30 am - 12.30 pm",
#     "8:30 am - 12:30 pm",
#     "12-6",
#     "8 am - 12 pm"
# ]

# for text in texts:
#     time_diff = extract_time_difference(text)
#     print(f"'{text}' => {time_diff} hours")


def count_working_days(text: str) -> int:
    """
    Tries to figure out the number of working days in a week, based on the days mentioned in free text.

    Uses regex plus a mapping of day names and their abbreviations to search for the pattern
    <day name> - <day name> or individual day names eg "Mon, Tue, Wed".

    Args:
        text (str): The input text containing day names or ranges of days.

    Returns:
        int: The number of unique working days mentioned in the text.
    """

    # Define day mappings to handle various abbreviations
    day_mappings = {
        "monday": 0,
        "mon": 0,
        "mondays": 0,
        "tuesday": 1,
        "tue": 1,
        "tues": 1,
        "tuesdays": 1,
        "wednesday": 2,
        "wed": 2,
        "wednesdays": 2,
        "thursday": 3,
        "thu": 3,
        "thurs": 3,
        "thursdays": 3,
        "friday": 4,
        "fri": 4,
        "fridays": 4,
        "saturday": 5,
        "sat": 5,
        "saturdays": 5,
        "sunday": 6,
        "sun": 6,
        "sundays": 6,
    }

    # Handle ranges like "Mon - Thurs" "Monday to Thursday"
    range_pattern = re.compile(r"(\b\w+\b)\s*(-|to)\s*(\b\w+\b)", re.IGNORECASE)
    # Handle individual days or day lists like "Mondays, Tuesdays and Fridays"
    individual_days_pattern = re.compile(r"\b\w+\b", re.IGNORECASE)

    days: Set[int] = set()

    # Check for ranges
    if len(range_pattern.findall(text)) > 0:
        for match in range_pattern.findall(text):
            # match 0 will be the first day in the range
            start_day = day_mappings.get(match[0].lower())
            # match 1 will be "to/-" so match 2 is the final day in the range
            end_day = day_mappings.get(match[2].lower())

            if start_day is not None and end_day is not None:
                # Add all days in the range to the set
                if start_day <= end_day:
                    for day in range(start_day, end_day + 1):
                        days.add(day)
                else:
                    # Handles cases where the range might go from end of the week to the start (e.g., Fri - Mon)
                    for day in range(start_day, 7):
                        days.add(day)
                    for day in range(0, end_day + 1):
                        days.add(day)
    else:
        # Check for individual days
        for day in individual_days_pattern.findall(text):
            day_index = day_mappings.get(day.lower())
            if day_index is not None:
                days.add(day_index)

    return len(days)


# # Example usage
# texts = [
#     "Monday - Thursday",
#     "Mon - Thurs",
#     "Mondays, Tuesdays and Fridays",
#     "Fri - Mon",
#     "Tuesday to Friday",
#     "Saturday - Sunday",
#     "Wed, Thu, Fri"
# ]

# for text in texts:
#     day_count = count_working_days(text)
#     print(f"'{text}' => {day_count} days")


def check_full_time(text: str) -> bool:
    """
    Checks if the given text contains the phrase "full time" or "full-time",
    case insensitive.

    Args:
        text (str): The input text to check.

    Returns:
        bool: True if "full time" or "full-time" is found in the text,
              False otherwise.
    """
    return "full time" in text.lower() or "full-time" in text.lower()


def calculate_hr_per_week_final(
    row: pd.Series, full_time_hrs: float = 37.5, hrs_per_day: float = 7.5
) -> Optional[float]:
    """
    Calculates the estimated hours per week based on the available information in the row.

    Before using this function, the row must contain the following fields:
    - 'hours_per_week': A list of dictionaries containing 'min_hours' and 'max_hours', created by `match_hours_per_week()`.
    - 'is_full_time': A boolean indicating whether the job is full-time, created by `check_full_time()`.
    - 'working_days': An integer indicating the number of working days, created by `count_working_days()`.

    The calculation follows these rules:

    1. If 'hours_per_week' is available, the function returns the 'min_hours' value from the first entry.
    2. If 'is_full_time' is True, the function returns full_time_hrs.
    3. If 'working_days' is available and greater than 0, the function returns 'working_days' * hrs_per_day.
    4. If none of the above rules apply, the function returns None.

    Args:
        row (pd.Series): A row from a DataFrame containing job information, including
                         'hours_per_week', 'is_full_time', and 'working_days'.
        full_time_hrs (float, optional): The number of hours per week for a full-time job. Defaults to 37.5.
        hrs_per_day (float, optional): The number of hours in a working day. Defaults to 7.5.

    Returns:
        Optional[float]: The estimated number of hours per week, or None if no information is available.
    """
    # Rule 1: Use 'hours_per_week' if available
    if row["hours_per_week"]:
        # Assuming 'hours_per_week' contains a list of dictionaries with 'min_hours' and 'max_hours'
        return row["hours_per_week"][0][
            "min_hours"
        ]  # or average, max, etc., depending on your requirement
    # Rule 2: Use 37.5 hours if 'is_full_time' is True
    elif row["is_full_time"]:
        return full_time_hrs
    # Rule 3: Use 'working_days' multiplied by 7.5
    elif isinstance(row["working_days"], int) and row["working_days"] > 0:
        return row["working_days"] * hrs_per_day
    # If none of the above rules apply, return None
    else:
        return None


# CONTRACT


def determine_contract_type(types: List[str]):
    if "Temporary" in types:
        return "Temporary"
    else:
        # Count occurrences of each type
        type_counts = pd.Series(types).value_counts()
        most_common = type_counts.idxmax()
        if len(type_counts) == 1:  # Only one unique type
            return most_common
        elif len(type_counts) > 1:
            # If the list contains more than one type, we need to check the most common
            if most_common == "Permanent" or most_common == "Unknown":
                return most_common
            else:
                return "Unknown"  # Fallback to 'Unknown' if neither 'Temporary' nor most frequent matches
        return "Unknown"


# Other utils shared across AFS analysis and healthcare analysis


def process_jq_data(processed_ads):
    """
    Merge the processed job adverts with the lookup table to get the subcategory and dimension of the target phrase.

    Create a wide version of the data (dimensions_wide) with one row per job ID.
    """

    lookup = get_keywords()

    processed_ads = processed_ads[
        ["id", "sentences_split", "target_phrase"]
    ].drop_duplicates()
    processed_ads = pd.merge(
        processed_ads,
        lookup[["target_phrase", "subcategory", "dimension"]],
        on="target_phrase",
        how="left",
    )
    dimensions_wide = create_wide_table(processed_ads)

    return processed_ads, dimensions_wide


def merge_jq_data(afs_raw_sample, dimensions_wide):
    """
    Bring the metadata for the job ads together with the boolean job quality columns
    """
    afs_sample = pd.merge(afs_raw_sample, dimensions_wide, on="id", how="left")
    columns_to_replace = dimensions_wide.columns[2:]  # the first column is the id
    # These columns have NaN where there are *no* mentions of JQ dimensions in these job adverts
    afs_sample[columns_to_replace] = afs_sample[columns_to_replace].fillna(0)
    return afs_sample
