#!/usr/bin/env python3
"""Write forex history."""

import pandas as pd

import common


def main():
    """Get and update forex data."""
    forex_df_data = {}
    for ticker, price in common.get_tickers(["CHFUSD=X", "SGDUSD=X"]).items():
        forex_df_data[ticker.split("=")[0]] = price
    forex_df = pd.DataFrame(
        forex_df_data, index=[pd.Timestamp.now()], columns=list(forex_df_data.keys())
    )
    common.to_sql(forex_df, "forex")


if __name__ == "__main__":
    main()
