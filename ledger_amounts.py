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
LEDGER_BALANCE_MULTI_CURRENCY_CMD = (
    f"{common.LEDGER_BIN} -f {common.LEDGER_DAT} " "-c -n -J bal"
)
LEDGER_LIMIT_ETFS = '--limit "commodity=~/^(SCH|SW[AIT]|GLD|SGOL|SIVR|IBKR)/"'


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


def write_balance(ledger_args, filename, multi_currency=False):
    """Write balance to file."""
    if multi_currency:
        cmd = f"{LEDGER_BALANCE_MULTI_CURRENCY_CMD} {ledger_args}"
    else:
        cmd = f"{LEDGER_BALANCE_CMD} {ledger_args}"
    process = subprocess.run(
        cmd,
        shell=True,
        check=True,
        text=True,
        capture_output=True,
    )
    if multi_currency:
        amount = process.stdout.split()[-1]
    else:
        amount = process.stdout
    try:
        value = float(amount)
    except ValueError:
        value = 0
    with common.temporary_file_move(f"{common.PREFIX}{filename}") as output_file:
        output_file.write(f"{value}\n")


def write_balances():
    """Write all balances to files."""
    write_balance('"^Assets:Ally Checking"', "ally.txt")
    write_balance('"^Assets:Apple Cash"', "apple_cash.txt")
    write_balance('"^Assets:Bank of America Checking"', "bofa.txt")
    write_balance('"^Assets:Charles Schwab Checking"', "schwab_checking.txt")
    write_balance('"^Assets:UBS Personal Account"', "ubs.txt")
    write_balance('"^Assets:Wealthfront Cash"', "wealthfront_cash.txt")
    write_balance('"^Assets:Zurcher Kantonal"', "zurcher.txt")
    write_balance(
        '"^Assets:Investments:Retirement:UBS Vested Benefits"', "ubs_pillar2.txt"
    )
    write_balance(
        "-X '$' --limit 'commodity=~/(SWVXX|\\\\$)/' -n "
        + '"^Assets:Investments:Charles Schwab .*Brokerage"',
        "schwab_brokerage_cash.txt",
    )
    write_balance(
        "-X '$' --limit 'commodity=~/^(CHF|\\\\$)/' " + '"Interactive Brokers"',
        "interactive_brokers_cash.txt",
        multi_currency=True,
    )
    write_balance("--limit 'commodity == \"CHF\"' " + '"^Assets:Cash"', "cash_chf.txt")
    write_balance("--limit 'commodity == \"CHF\"' " + '"^Assets:Wise"', "wise_chf.txt")
    write_balance("--limit 'commodity == \"GBP\"' " + '"^Assets:Wise"', "wise_gbp.txt")
    write_balance("--limit 'commodity == \"SGD\"' " + '"^Assets:Wise"', "wise_sgd.txt")
    write_balance("--limit 'commodity == \"$\"' " + '"^Assets:Wise"', "wise_usd.txt")
    write_balance('"^Liabilities:Apple Card"', "apple_card.txt")
    write_balance(
        '"^Liabilities:Bank of America Travel Rewards Credit Card"',
        "bofa_travel_rewards_visa.txt",
    )
    write_balance(
        '"^Liabilities:Bank of America Cash Rewards Credit Card"',
        "bofa_cash_rewards_visa.txt",
    )
    write_balance('"^Liabilities:Charles Schwab PAL"', "schwab_pledged_asset_line.txt")
    write_balance('"^Liabilities:UBS Visa"', "ubs_visa.txt")
    write_balance('"^Assets:Rental Property:Reserve"', "rental_property_reserve.txt")


def main():
    """Main."""
    write_balances()
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
