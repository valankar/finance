#!/usr/bin/env python3
"""Store Schwab SWVXX 7-day yield history."""

import pandas as pd

import common


def get_yield():
    """Get 7 day yield with Selenium."""
    return float(
        common.find_xpath_via_browser(
            "https://www.schwabassetmanagement.com/products/swvxx",
            '//*[@id="sfm-table--yields"]/table/tbody/tr[1]/td[2]',
        ).strip("%")
    )


def main():
    """Writes 7 day yield history to CSV file."""
    new_df = pd.DataFrame({"percent": get_yield()}, index=[pd.Timestamp.now()])
    common.to_sql(new_df, "swvxx_yield")


if __name__ == "__main__":
    main()
