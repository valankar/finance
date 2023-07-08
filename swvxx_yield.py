#!/usr/bin/env python3
"""Store Schwab SWVXX 7-day yield history."""

import pandas as pd
from sqlalchemy import create_engine

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
    with create_engine(common.SQLITE_URI).connect() as conn:
        new_df.to_sql("swvxx_yield", conn, if_exists="append", index_label="date")
        conn.commit()


if __name__ == "__main__":
    main()
