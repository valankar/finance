#!/usr/bin/env python3
"""Store IBKR margin rate history for USD, CHF."""

import pandas as pd
import requests
from bs4 import BeautifulSoup

import common


def main():
    """Writes 7 day yield history to CSV file."""
    url = "https://www.interactivebrokers.com/en/trading/margin-rates.php"
    soup = BeautifulSoup(requests.get(url, timeout=60).content, "html.parser")
    usd = float(
        soup.select(
            # pylint: disable-next=line-too-long
            "#interest-schedule > div > div:nth-child(3) > div > div > table > tbody > tr:nth-child(1) > td:nth-child(3) > span > span:nth-child(1)"
        )[0].text.strip("%")
    )
    chf = float(
        soup.select(
            # pylint: disable-next=line-too-long
            "#interest-schedule > div > div:nth-child(3) > div > div > table > tbody > tr:nth-child(20) > td:nth-child(3) > span > span:nth-child(1)"
        )[0].text.strip("%")
    )
    new_df = pd.DataFrame({"USD": usd, "CHF": chf}, index=[pd.Timestamp.now()])
    common.to_sql(new_df, "interactive_brokers_margin_rates")


if __name__ == "__main__":
    main()
