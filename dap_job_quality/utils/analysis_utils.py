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


### HOURLY PAY ###


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

### CONTRACT ###

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


### HOURS ###


def match_hours_per_week(text):

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


def parse_time(time_str):
    # Define time formats to match various input patterns
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


def extract_time_difference(text):
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


def count_working_days(text):
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

    # Handle ranges like "Mon - Thurs"
    range_pattern = re.compile(r"(\b\w+\b)\s*(-|to)\s*(\b\w+\b)", re.IGNORECASE)
    # Handle individual days or day lists like "Mondays, Tuesdays and Fridays"
    individual_days_pattern = re.compile(r"\b\w+\b", re.IGNORECASE)

    days = set()

    # Check for ranges
    if len(range_pattern.findall(text)) > 0:
        # print(range_pattern.findall(text))
        for match in range_pattern.findall(text):
            start_day = day_mappings.get(match[0].lower())
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

    # print(days)
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


def check_full_time(text):
    return "full time" in text.lower() or "full-time" in text.lower()


def calculate_hr_per_week_final(row):
    """Before applying this function, you need to have already used the following functions to extract
    the relevant info as follows:
    - row['hours_per_week'] - created by match_hours_per_week()
    - row['is_full_time'] - created by check_full_time()
    - row['working_days'] - created by count_working_days()
    """
    # Rule 1: Use 'hours_per_week' if available
    if row["hours_per_week"]:
        # Assuming 'hours_per_week' contains a list of dictionaries with 'min_hours' and 'max_hours'
        return row["hours_per_week"][0][
            "min_hours"
        ]  # or average, max, etc., depending on your requirement
    # Rule 2: Use 37.5 hours if 'is_full_time' is True
    elif row["is_full_time"]:
        return 37.5  # or another standard full-time hours value
    # Rule 3: Use 'working_days' multiplied by 7.5
    elif isinstance(row["working_days"], int) and row["working_days"] > 0:
        return row["working_days"] * 7.5
    # If none of the above rules apply, return None or 0
    else:
        return None
