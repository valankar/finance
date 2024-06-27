#!/usr/bin/env python3
"""Find collar option strategies."""

import io
import math
import subprocess
import sys
from collections import defaultdict

import yahooquery

import common


def collars_needed(brokerage):
    """Find out which tickers need collars."""
    cmd = f"{common.LEDGER_BIN} -f {common.LEDGER_DAT} --limit 'commodity=~/(^S|PUT)/' bal '{brokerage}'"
    ticker_values = defaultdict(list)
    for line in io.StringIO(subprocess.check_output(cmd, shell=True, text=True)):
        amount, ticker = line.strip().split(maxsplit=1)
        if " PUT" in ticker:
            amount = float(amount) * -100
        else:
            amount = float(amount)
        ticker = ticker.split()[0].strip('"')
        ticker_values[ticker].append(amount)
    options_needed = set()
    for ticker, values in ticker_values.items():
        total = sum(values)
        if total > 100:
            print(f"{ticker} can get {math.floor(total/100)} options at {brokerage}")
            options_needed.add(ticker)
    return options_needed


def find_collar_options(ticker):
    """Find profitable collars."""
    t = yahooquery.Ticker(ticker)
    df = t.option_chain
    price = t.price[ticker]["regularMarketPrice"]
    print(f"\nTicker: {ticker}\nPrice: {price}")
    df["midpoint"] = (df["bid"] + df["ask"]) / 2
    otm = df[(df["inTheMoney"] == False) & (df["midpoint"] > 0)]
    calls = otm.xs("calls", level=2)
    calls = calls[calls["strike"] > price]
    puts = otm.xs("puts", level=2)
    puts = puts[puts["strike"] < price]
    for call_date_index in calls.index.drop_duplicates():
        for call in calls.loc[call_date_index].itertuples():
            try:
                puts_at_date = puts.loc[[call_date_index]]
            except KeyError:
                continue
            possible_puts = puts_at_date[(call.midpoint - puts_at_date["midpoint"]) > 0]
            if len(possible_puts):
                print(f"\n{call.Index[1]}")
                print(f"CALL {call.strike}")
                for put in possible_puts.itertuples():
                    print(
                        f"  PUT {put.strike} with midpoint limit credit {call.midpoint - put.midpoint:.2f}"
                    )


def main():
    """Main."""
    if len(sys.argv) > 1:
        needed_tickers = set(sys.argv[1:])
    else:
        needed_tickers = collars_needed("Interactive Brokers") | collars_needed(
            "Schwab Brokerage"
        )
    for ticker in sorted(needed_tickers):
        find_collar_options(ticker)


if __name__ == "__main__":
    main()
