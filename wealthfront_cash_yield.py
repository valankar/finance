#!/usr/bin/env python3
"""Store Wealthfront Cash yield history."""

from datetime import date, datetime

import pandas as pd
from sqlalchemy import create_engine

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
    new_df = pd.DataFrame(
        {"percent": get_yield() + boost}, index=pd.DatetimeIndex([datetime.now()])
    )
    with create_engine(common.SQLITE_URI).connect() as conn:
        new_df.to_sql(
            "wealthfront_cash_yield", conn, if_exists="append", index_label="date"
        )
        conn.commit()


if __name__ == "__main__":
    main()
