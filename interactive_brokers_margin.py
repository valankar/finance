#!/usr/bin/env python3
"""Store IBKR margin rate history for USD, CHF."""

import pandas as pd

import common

# pylint: disable=line-too-long


def main():
    """Writes IB margin rates to DB."""
    usd, chf = [
        float(x.strip("%"))
        for x in common.find_xpaths_via_browser(
            "https://www.interactivebrokers.com/en/trading/margin-rates.php",
            [
                '//*[@id="interest-schedule"]/div/div[3]/div/div/table/tbody/tr[1]/td[3]/span/span[1]',
                '//*[@id="interest-schedule"]/div/div[3]/div/div/table/tbody/tr[20]/td[3]/span/span[1]',
            ],
        )
    ]
    new_df = pd.DataFrame({"USD": usd, "CHF": chf}, index=[pd.Timestamp.now()])
    common.to_sql(new_df, "interactive_brokers_margin_rates")


if __name__ == "__main__":
    main()
