#!/usr/bin/env python
"""Update balances in DB and text files."""

import subprocess

import pandas as pd

import common

LEDGER_COMMODITY_CMD = (
    f"{common.LEDGER_BIN} -f {common.LEDGER_DAT} "
    '--balance-format "%(S(display_total))" -n -c bal'
)
LEDGER_BALANCE_CMD = (
    f"{common.LEDGER_BIN} -f {common.LEDGER_DAT} "
    '--balance-format "%(quantity(scrub(display_total)))" -c bal'
)
LEDGER_LIMIT_ETFS = (
    f"""--limit 'commodity!~/{common.CURRENCIES_REGEX}/ and commodity=~/^[A-Z]+/'"""
)


def get_commodity_df(ledger_args: str) -> pd.DataFrame | None:
    process = subprocess.run(
        f"{LEDGER_COMMODITY_CMD} {ledger_args}",
        shell=True,
        check=True,
        text=True,
        capture_output=True,
    )

    lines = process.stdout.splitlines()
    df_data = {}
    for line in lines:
        shares, ticker = line.split(maxsplit=1)
        ticker = ticker.strip('"')
        df_data[ticker] = float(shares)
    if not df_data:
        return None
    return pd.DataFrame(
        df_data, index=[pd.Timestamp.now()], columns=sorted(df_data.keys())
    ).rename_axis("date")


def write_commodity(ledger_args, table):
    """Write commodity table."""
    if (dataframe := get_commodity_df(ledger_args)) is not None:
        common.to_sql(dataframe, table)


def main():
    """Main."""
    write_commodity(
        LEDGER_LIMIT_ETFS
        + ' --limit "account=~/^Assets:Investments:(Charles Schwab .*Brokerage|Interactive Brokers)/"',
        "schwab_etfs_amounts",
    )
    write_commodity(
        '--limit "commodity=~/^SWYGX/" ^"Assets:Investments:Retirement:Charles Schwab IRA"',
        "schwab_ira_amounts",
    )


if __name__ == "__main__":
    main()
