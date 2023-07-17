#!/usr/bin/env python3
"""Store Wealthfront Cash yield history."""

from datetime import date, datetime

import pandas as pd

import common

# End date, how much to boost.
APY_BOOST = ("2023-11-17", 0.50)


def get_yield():
    """Get yield from Wealthfront support page with Selenium."""
    return float(
        common.find_xpath_via_browser(
            # pylint: disable-next=line-too-long
            "https://support.wealthfront.com/hc/en-us/articles/360043680212-Interest-rate-for-Cash-Accounts",
            "/html/body/main/div[1]/article/div[1]/p[1]/strong[1]",
        )
    )


def main():
    """Writes 7 day yield history to database."""
    today = date.today()
    boost = 0
    if today <= datetime.strptime(APY_BOOST[0], "%Y-%m-%d").date():
        boost = APY_BOOST[1]
    new_df = pd.DataFrame({"percent": get_yield() + boost}, index=[pd.Timestamp.now()])
    common.to_sql(new_df, "wealthfront_cash_yield")


if __name__ == "__main__":
    main()
