"""Common Date Utility."""

# import: standard
import glob
from typing import Set

# import: external
import pandas


def get_holiday() -> list:
    """Method to get list of all distinct holiday dates from holiday files.

    Returns:
        list: Distinct holiday date list
    """
    file_pattern = "/datasource/inbound/source_file/mdp/sfv/holiday_*.txt"

    # Get all files matching the pattern and store distinct dates as set
    holiday_files = glob.glob(file_pattern)
    holiday_dates: Set[str] = set()

    for file_path in holiday_files:
        with open(file_path, "r") as file:
            # Add dates from the current file to the set
            holiday_dates.update(line.strip() for line in file.readlines())

    return sorted(holiday_dates)


def get_offset_businessdays(date_str: str, holidays: list = [], offset=1) -> str:
    """Calculate the previous business day(s) excluding weekends and specified holidays.

    This function takes an input date, a list of holidays, and a number of days to subtract.
    It returns the date after subtracting the specified number of business days, skipping weekends
    and the holidays provided (Optional).

    Args:
        date_str (str): The input date as a string in the format %Y-%m-%d
        holidays (list, Optional):  A list of dates (in string format) that should be treated as holidays.
        offset (int, optional):  The number of business days offset from the input date. Defaults to 1.

    Returns:
        str: The date after subtracting the specified number of business days,
    """

    # Convert the input date string and holidays to pandas Timestamps
    date = pandas.to_datetime(date_str, format="%Y-%m-%d")
    if holidays:
        holiday_dates = pandas.to_datetime(holidays, format="%Y-%m-%d")

        # Create a custom business day offset that excludes weekends and the provided holidays
        custom_business_day = pandas.offsets.CustomBusinessDay(holidays=holiday_dates)

        # Subtract the specified number of custom business days
        previous_weekday = date + custom_business_day * offset
    else:
        previous_weekday = date + pandas.offsets.BDay(offset)

    # Return the previous weekday as a string in the same format
    return previous_weekday.strftime("%Y-%m-%d")
