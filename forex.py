#!/usr/bin/env python3
"""Write forex history."""

import common


def main():
    """Get and update forex data."""
    forex_df_data = {}
    for ticker, price in common.get_tickers(["CHFUSD=X", "SGDUSD=X"]).items():
        forex_df_data[ticker.split("=")[0]] = price
    common.insert_sql("forex", forex_df_data)


if __name__ == "__main__":
    main()
