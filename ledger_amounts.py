#!/usr/bin/env python

import subprocess
from typing import Optional

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


def get_commodity_amounts(ledger_args: str) -> dict[str, float]:
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
    return df_data


def get_etfs_amounts(account: Optional[str] = None) -> dict[str, float]:
    if account:
        return get_commodity_amounts(
            LEDGER_LIMIT_ETFS + f' --limit "account=~/^Assets:Investments:{account}/"'
        )

    brokers = get_commodity_amounts(
        LEDGER_LIMIT_ETFS
        + ' --limit "account=~/^Assets:Investments:(Charles Schwab .*Brokerage|Interactive Brokers)/"'
    )
    ira = get_commodity_amounts(
        LEDGER_LIMIT_ETFS
        + ' --limit "account=~/^Assets:Investments:Retirement:Charles Schwab IRA/"'
    )
    return brokers | ira
