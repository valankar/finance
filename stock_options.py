#!/usr/bin/env python3
"""Methods for stock options."""

import io
import subprocess

import pandas as pd

import common
import etfs


def options_df():
    """Get call and put dataframe."""
    cmd = (
        f"{common.LEDGER_BIN} -f {common.LEDGER_DAT} --limit 'commodity=~/ (CALL|PUT)/' bal "
        + '--no-total --flat --balance-format "%(partial_account)\n%(strip(T))\n"'
    )
    entries = []
    for line in io.StringIO(subprocess.check_output(cmd, shell=True, text=True)):
        if line[0].isalpha():
            account = line.strip()
            continue
        count = line.split(maxsplit=1)[0]
        call_name = line.split(maxsplit=1)[1].strip().strip('"')
        ticker = call_name.split()[0]
        option_type = call_name.split()[-1]
        strike = call_name.split()[-2]
        expiration = call_name.split()[-3]
        entries.append(
            {
                "name": call_name,
                "type": option_type,
                "ticker": ticker,
                "count": int(count),
                "strike": int(strike),
                "expiration": pd.to_datetime(expiration),
                "account": account,
            }
        )
    calls_df = pd.DataFrame(entries)
    etfs_df = pd.read_csv(
        etfs.CSV_OUTPUT_PATH, index_col=0, usecols=["ticker", "current_price"]
    ).fillna(0)
    joined_df = pd.merge(calls_df, etfs_df, on="ticker").set_index(
        ["account", "name", "expiration"]
    )
    joined_df.loc[joined_df["type"] == "CALL", "in_the_money"] = (
        joined_df["strike"] < joined_df["current_price"]
    )
    joined_df.loc[joined_df["type"] == "PUT", "in_the_money"] = (
        joined_df["strike"] > joined_df["current_price"]
    )
    joined_df["exercise_value"] = joined_df["strike"] * joined_df["count"] * 100

    joined_df.loc[
        (joined_df["type"] == "CALL") & (joined_df["count"] < 0), "exercise_value"
    ] = abs(joined_df["strike"] * joined_df["count"] * 100)
    joined_df.loc[
        (joined_df["type"] == "PUT") & (joined_df["count"] < 0),
        "exercise_value",
        # pylint: disable-next=superfluous-parens
    ] = (abs(joined_df["strike"] * joined_df["count"] * 100) * -1)
    return joined_df.sort_values(["account", "expiration", "name"])


def short_put_exposure(dataframe, broker):
    """Get exposure of short puts along with long puts."""
    broker_puts = dataframe.xs(broker, level="account").loc[
        lambda df: df["type"] == "PUT"
    ]
    broker_short_puts = broker_puts[broker_puts["count"] < 0]
    total = 0
    for index, _ in broker_short_puts.iterrows():
        ticker_date = " ".join(index[0].split()[0:2])
        total += sum(broker_puts.filter(like=ticker_date, axis=0)["exercise_value"])
    return total


def main():
    """Main."""
    options = options_df()
    print("Out of the money")
    # pylint: disable-next=singleton-comparison
    print(options[options["in_the_money"] == False])
    print("\nIn the money")
    print(options[options["in_the_money"]], "\n")
    for broker in ["Charles Schwab Brokerage", "Interactive Brokers"]:
        print(f"{broker}")
        print(f"  Short put exposure: {short_put_exposure(options, broker)}")
        print(options.xs(broker, level="account"))
        print("\n")


if __name__ == "__main__":
    main()
