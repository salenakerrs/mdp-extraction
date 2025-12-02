"""Test for Utillities Date Common Module."""
# import: standard
import logging
from unittest.mock import mock_open
from unittest.mock import patch

# import: internal
from mdp.framework.mdp_extraction_framework.utility.date.common import get_holiday
from mdp.framework.mdp_extraction_framework.utility.date.common import get_offset_businessdays

# import: external
import pytest

logger = logging.getLogger(__name__)


def test_get_holiday():
    # Mock file data for multiple files
    file_data_1 = "2024-01-01\n2024-01-02\n2024-01-03"
    file_data_2 = "2024-01-03\n2024-01-04\n2024-01-05"

    # Mock glob to return a list of file paths
    mock_files = ["/path/to/holiday_1.txt", "/path/to/holiday_2.txt"]
    with patch("glob.glob", return_value=mock_files):
        # Mock open to read file contents
        with patch("builtins.open", mock_open()) as mocked_open:
            mocked_open.side_effect = [
                mock_open(read_data=file_data_1).return_value,
                mock_open(read_data=file_data_2).return_value,
            ]

            # Call the method
            result = get_holiday()

            # Verify the result
            assert result == ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"]

            # Verify open was called for each file
            mocked_open.assert_any_call("/path/to/holiday_1.txt", "r")
            mocked_open.assert_any_call("/path/to/holiday_2.txt", "r")


@pytest.mark.parametrize(
    "date_str, holidays, offset, expected_pos_dt",
    [
        ("2024-08-19", [], 1, "2024-08-20"),  # Next business day (Tuesday)
        ("2024-08-19", [], -1, "2024-08-16"),  # Previous business day (Friday)
        ("2024-08-19", ["2024-08-19"], 1, "2024-08-20"),  # Skip holiday, move to Tuesday
        ("2024-08-20", ["2024-08-19"], -1, "2024-08-16"),  # Skip holiday, move to Friday
        (
            "2024-08-19",
            ["2024-08-19", "2024-08-20"],
            1,
            "2024-08-21",
        ),  # Skip both holidays, move to Wednesday
        (
            "2024-08-21",
            ["2024-08-19", "2024-08-20"],
            -2,
            "2024-08-15",
        ),  # Skip both holidays, move to previous Friday
        (
            "2024-08-30",
            ["2024-08-30"],
            1,
            "2024-09-02",
        ),  # Skip holiday and weekend, move to next Monday
        ("2024-08-19", [], 0, "2024-08-19"),  # No offset, should return the same date
    ],
    ids=[
        "next_business_day_no_holidays",
        "previous_business_day_no_holidays",
        "skip_holiday_forward",
        "skip_holiday_backward",
        "skip_multiple_holidays_forward",
        "skip_multiple_holidays_backward",
        "end_of_month_with_holiday",
        "no_offset",
    ],
)
def test_get_offset_businessdays(date_str, holidays, offset, expected_pos_dt):
    actual_pos_dt = get_offset_businessdays(date_str, holidays, offset)
    assert actual_pos_dt == expected_pos_dt
