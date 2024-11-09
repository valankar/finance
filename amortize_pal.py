#!/usr/bin/env python3
"""Calculate the maximum balance on pledged asset line given a monthly payment."""

import argparse
import io
import subprocess

import numpy_financial as npf
from amortization.schedule import amortization_schedule
from tabulate import tabulate

import common
import ledger_amounts

# APY is SOFR plus this amount
APY_OVER_SOFR_SCHWAB = 2.80
APY_OVER_SOFR_IBKR = 1.53

# How long to amortize over.
MONTHS = 12

# pylint: disable=line-too-long
# Ledger command to get monthly investment income.
LEDGER_INCOME_BASE_CMD = (
    f"{common.LEDGER_PREFIX} -D -n -A -b '2023' --tail 1 -J register"
)
LEDGER_INCOME_ALL_CMD = f"{LEDGER_INCOME_BASE_CMD} \\(^Income:Dividends or ^Income:Interest or ^Income:Grants\\)"
LEDGER_INCOME_SCHWAB_CMD = f"{LEDGER_INCOME_ALL_CMD} and payee Schwab"
LEDGER_INCOME_IBKR_CMD = f"{LEDGER_INCOME_ALL_CMD} and payee Interactive"
LEDGER_LOAN_BALANCE_HISTORY_IBKR = (
    f"{common.LEDGER_PREFIX} "
    + r"""--limit 'commodity=~/^(\\$|CHF|"SPX)/' -J -E reg ^Assets:Investments:'Interactive Brokers'"""
)
LEDGER_BALANCE_HISTORY_IBKR = (
    f"{common.LEDGER_PREFIX} "
    + f"--limit 'commodity=~/{ledger_amounts.ETFS_REGEX}/' -J -E reg ^Assets:Investments:'Interactive Brokers'"
)
LEDGER_BALANCE_HISTORY_SCHWAB_NONPAL = (
    f"{common.LEDGER_PREFIX} "
    + f"--limit 'commodity=~/{ledger_amounts.ETFS_REGEX}|^SWVXX/' -J -E reg ^Assets:Investments:'Charles Schwab Brokerage'"
)
LEDGER_LOAN_BALANCE_HISTORY_SCHWAB_NONPAL = (
    f"{common.LEDGER_PREFIX} "
    + r"""--limit 'commodity=~/^(\\$|"SPX)/' -J -E -b 2024-03-26 reg ^Assets:Investments:'Charles Schwab Brokerage'"""
)


def get_args():
    """Get command line arguments."""
    parser = argparse.ArgumentParser(
        prog="amortize_pal",
        description="Determine maximum PAL balance given monthly payments.",
    )
    parser.add_argument("--brokerage", choices=["ibkr", "schwab"])
    parser.add_argument("--apy", default=None, type=float)
    parser.add_argument("--apy_over_sofr", default=None, type=float)
    parser.add_argument("--monthly_payment", default=None, type=int)
    parser.add_argument("--months", default=MONTHS, type=int)
    return parser.parse_args()


def get_monthly_investment_income(income_cmd):
    """Get monthly dividend and interest income."""
    amount = round(
        abs(
            float(
                list(
                    io.StringIO(
                        subprocess.check_output(income_cmd, shell=True, text=True)
                    )
                )[0].split()[1]
            )
        )
        * 30,
    )
    return amount


def get_max_loan_interest_only(apy, months, monthly_payment):
    """Get maximum interest-only loan given by monthly_payment over months. Apy is percentage."""
    return round((monthly_payment * months) / apy)


def get_max_loan(apy, months, monthly_payment):
    """Get maximum loan given by monthly_payment over months. Apy is percentage."""
    return round(abs(npf.pv(apy / months, months, monthly_payment)), 2)  # type: ignore


def get_sofr():
    """Get latest SOFR."""
    return common.read_sql_table("sofr").iloc[-1]["percent"]


def main():
    """Main."""
    args = get_args()
    apy_over_sofr = args.apy_over_sofr
    monthly_payment = args.monthly_payment
    match args.brokerage:
        case "ibkr":
            apy_over_sofr = APY_OVER_SOFR_IBKR
            income_cmd = LEDGER_INCOME_IBKR_CMD
        case "schwab":
            apy_over_sofr = APY_OVER_SOFR_SCHWAB
            income_cmd = LEDGER_INCOME_SCHWAB_CMD
        case _:
            apy_over_sofr = APY_OVER_SOFR_SCHWAB
            income_cmd = LEDGER_INCOME_ALL_CMD
    monthly_payment = get_monthly_investment_income(income_cmd)
    sofr = get_sofr()
    if args.apy:
        apy = args.apy
    else:
        apy = sofr + apy_over_sofr
    apy /= 100
    amount = get_max_loan(apy, args.months, monthly_payment)
    table = (x for x in amortization_schedule(amount, apy, args.months))
    loan_text = (
        f"Loan of ${amount:,} would result in {args.months} monthly payments "
        f"of ${monthly_payment:,} at {apy*100:.2f}% APY"
    )
    if not args.apy:
        loan_text += f" ({sofr} (SOFR) + {apy_over_sofr})"
    print(f"Monthly dividend and interest income: ${monthly_payment}")
    print(loan_text)
    print(
        tabulate(
            table,
            headers=["Number", "Amount", "Interest", "Principal", "Balance"],
            floatfmt=",.2f",
            numalign="right",
        )
    )
    amount_interest_only = get_max_loan_interest_only(apy, args.months, monthly_payment)
    print(f"Loan of ${amount_interest_only:,} with interest-only payment.")


if __name__ == "__main__":
    main()
