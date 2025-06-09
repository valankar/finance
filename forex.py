#!/usr/bin/env python3
"""Write forex history."""

from loguru import logger

import common


def get_ticker(ticker: str) -> float:
    for f in (common.get_ticker, common.get_ticker_all):
        try:
            return f(ticker)
        except common.GetTickerError:
            pass
    raise common.GetTickerError("All get_ticker methods failed")


def main():
    """Get and update forex data."""
    forex_df_data = {}
    for ticker in ("CHFUSD=X", "SGDUSD=X"):
        try:
            forex_df_data[ticker.split("=")[0]] = get_ticker(ticker)
        except common.GetTickerError as e:
            logger.error(str(e))
            return
    common.insert_sql("forex", forex_df_data)


if __name__ == "__main__":
    main()
