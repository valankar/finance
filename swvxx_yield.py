#!/usr/bin/env python3
"""Store Schwab SWVXX 7-day yield history."""

import pandas as pd

import common


def get_yield():
    """Get 7 day yield with Selenium."""
    with common.run_with_browser_page(
        "https://www.schwabassetmanagement.com/products/swvxx"
    ) as page:
        common.schwab_browser_page(page)
        return float(
            page.get_by_role("row", name="7-Day Yield (with waivers) As")
            .get_by_role("cell")
            .inner_text()
            .strip("%")
        )


def main():
    """Writes 7 day yield history to CSV file."""
    new_df = pd.DataFrame({"percent": get_yield()}, index=[pd.Timestamp.now()])
    common.to_sql(new_df, "swvxx_yield")


if __name__ == "__main__":
    main()
