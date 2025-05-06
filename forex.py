#!/usr/bin/env python3
"""Write forex history."""

from loguru import logger

import common


def main():
    """Get and update forex data."""
    forex_df_data = {}
    for ticker in ("CHFUSD=X", "SGDUSD=X"):
        try:
            forex_df_data[ticker.split("=")[0]] = common.get_ticker(ticker)
        except common.GetTickerError as e:
            logger.error(str(e))
            return
    common.insert_sql("forex", forex_df_data)


if __name__ == "__main__":
    main()
