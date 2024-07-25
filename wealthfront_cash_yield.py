#!/usr/bin/env python3
"""Store Wealthfront Cash yield history."""

import re

import pandas as pd

import common


def get_yield():
    """Get yield from Wealthfront support page with Selenium."""
    with common.run_with_browser_page(
        # pylint: disable-next=line-too-long
        "https://support.wealthfront.com/hc/en-us/articles/360043680212-Interest-rate-for-Cash-Accounts"
    ) as page:
        return float(
            re.search(
                r"is ([\d\.]+)% as of",
                page.get_by_role("paragraph")
                .filter(has_text="The annual percentage yield")
                .inner_text(),
            )[1]
        )


def main():
    """Writes 7 day yield history to database."""
    new_df = pd.DataFrame({"percent": get_yield()}, index=[pd.Timestamp.now()])
    common.to_sql(new_df, "wealthfront_cash_yield")


if __name__ == "__main__":
    main()
