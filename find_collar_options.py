#!/usr/bin/env python3
"""Find collar option strategies."""

import io
import math
import subprocess
import sys
from collections import defaultdict

import pandas as pd
import yahooquery

import amortize_pal
import balance_etfs
import common
import etfs
import plot


def options_df():
    """Get call and put dataframe."""
    cmd = f"{common.LEDGER_BIN} -f {common.LEDGER_DAT} --limit 'commodity=~/ (CALL|PUT)/' bal -n"
    entries = []
    for line in io.StringIO(subprocess.check_output(cmd, shell=True, text=True)):
        count = line.split(maxsplit=1)[0]
        call_name = (
            line.split(maxsplit=1)[1].split("\n")[0].strip("Assets").strip().strip('"')
        )
        ticker = call_name.split()[0]
        option_type = call_name.split()[-1]
        strike = call_name.split()[-2]
        entries.append(
            {
                "name": call_name,
                "type": option_type,
                "ticker": ticker,
                "count": int(count),
                "strike": int(strike),
            }
        )
    calls_df = pd.DataFrame(entries)
    etfs_df = pd.read_csv(
        etfs.CSV_OUTPUT_PATH, index_col=0, usecols=["ticker", "current_price"]
    ).fillna(0)
    joined_df = pd.merge(calls_df, etfs_df, on="ticker").set_index("name")
    joined_df["current_price_minus_strike"] = (
        joined_df["current_price"] * joined_df["count"] * 100
    ) - (joined_df["strike"] * joined_df["count"] * 100)
    joined_df["in_the_money"] = joined_df["current_price_minus_strike"] < 0
    joined_df["exercise_value"] = abs(joined_df["strike"] * joined_df["count"] * 100)
    return joined_df.sort_values("current_price_minus_strike")


def collars_needed(brokerage):
    """Find out which tickers need collars."""
    # pylint: disable=line-too-long
    cmd = f"{common.LEDGER_BIN} -f {common.LEDGER_DAT} --limit 'commodity=~/(^SCH|SGOL| PUT| CALL)/' bal '{brokerage}'"
    ticker_values = defaultdict(list)
    call_amount = 0
    for line in io.StringIO(subprocess.check_output(cmd, shell=True, text=True)):
        amount, ticker = line.strip().split(maxsplit=1)
        if ticker in balance_etfs.ETF_TYPE_MAP["US_BONDS"]:
            continue
        if " PUT" in ticker:
            continue
        if " CALL" in ticker:
            call_strike = int(ticker.split("CALL")[-2].split()[-1])
            call_amount += int(amount) * -100 * call_strike
            amount = float(amount) * 100
        else:
            amount = float(amount)
        ticker = ticker.split()[0].strip('"')
        ticker_values[ticker].append(amount)
    options_needed = set()
    print(brokerage)
    for ticker, values in ticker_values.items():
        total = sum(values)
        if total > 100:
            print(f"  {ticker} can get {math.floor(total/100)} short call options")
            options_needed.add(ticker)
    if call_amount:
        print(f"  Short CALL value: ${call_amount}")
        if brokerage == "Interactive Brokers":
            loan_balance = int(
                plot.load_loan_balance_df(
                    amortize_pal.LEDGER_LOAN_BALANCE_HISTORY_IBKR
                ).iloc[-1]["Loan Balance"]
            )
            print(f"    Balance: ${loan_balance}")
            print(f"    Short CALL value + Balance: ${call_amount + loan_balance}")
    return options_needed


def find_collar_options(ticker):
    """Find profitable collars."""
    t = yahooquery.Ticker(ticker)
    df = t.option_chain
    if isinstance(df, str):
        print(f"{ticker}: {df}")
        return
    price = t.price[ticker]["regularMarketPrice"]
    print(f"\nTicker: {ticker}\nPrice: {price}")
    df["midpoint"] = (df["bid"] + df["ask"]) / 2
    otm = df[~df["inTheMoney"] & (df["bid"] > 0) & (df["ask"] > 0)]
    try:
        calls = otm.xs("calls", level=2)
        calls = calls[calls["strike"] > price]
    except KeyError:
        return
    print(calls)


def main():
    """Main."""
    if len(sys.argv) > 1:
        needed_tickers = set(sys.argv[1:])
    else:
        needed_tickers = collars_needed("Interactive Brokers") | collars_needed(
            "Schwab Brokerage"
        )
    options = options_df()
    print(options[options["in_the_money"]])
    for ticker in sorted(needed_tickers):
        find_collar_options(ticker)


if __name__ == "__main__":
    main()
