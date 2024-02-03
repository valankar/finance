#!/usr/bin/env python3
"""Calculate the maximum balance on pledged asset line given a monthly payment."""

import argparse
import io
import subprocess

from amortization.schedule import amortization_schedule
import numpy_financial
from tabulate import tabulate

import common

# APY is SOFR plus this amount (Schwab PAL)
APY_OVER_SOFR = 2.9

# How long to amortize over.
MONTHS = 12

# Ledger command to get monthly investment income.
# pylint: disable-next=line-too-long
LEDGER_INCOME_CMD = f"{common.LEDGER_PREFIX} -D -n -A -b '2023' --tail 1 -J register \\(^Income:Dividends or ^Income:Interest or ^Income:Grants\\)"


def get_args():
    """Get command line arguments."""
    parser = argparse.ArgumentParser(
        prog="amortize_pal",
        description="Determine maximum PAL balance given monthly payments.",
    )
    parser.add_argument("--apy", default=None, type=float)
    parser.add_argument("--apy_over_sofr", default=APY_OVER_SOFR, type=float)
    parser.add_argument(
        "--monthly_payment", default=get_monthly_investment_income(), type=int
    )
    parser.add_argument("--months", default=MONTHS, type=int)
    return parser.parse_args()


def get_monthly_investment_income():
    """Get monthly dividend and interest income."""
    amount = round(
        abs(
            float(
                list(
                    io.StringIO(
                        subprocess.check_output(
                            LEDGER_INCOME_CMD, shell=True, text=True
                        )
                    )
                )[0].split()[1]
            )
        )
        * 30,
    )
    return amount


def get_max_loan(apy, months, monthly_payment):
    """Get maximum loan given by monthly_payment over months. Apy is percentage."""
    return round(abs(numpy_financial.pv(apy / months, months, monthly_payment)), 2)


def get_sofr():
    """Get latest SOFR."""
    return common.read_sql_table("sofr").iloc[-1]["percent"]


def main():
    """Main."""
    args = get_args()
    print(f"Monthly dividend and interest income: ${args.monthly_payment}")
    sofr = get_sofr()
    if args.apy:
        apy = args.apy
    else:
        apy = sofr + args.apy_over_sofr
    apy /= 100
    amount = get_max_loan(apy, args.months, args.monthly_payment)
    table = (x for x in amortization_schedule(amount, apy, args.months))
    loan_text = (
        f"Loan of ${amount:,} would result in {args.months} monthly payments "
        f"of ${args.monthly_payment:,} at {apy*100:.2f}% APY"
    )
    if not args.apy:
        loan_text += f" ({sofr} (SOFR) + {args.apy_over_sofr})"
    print(loan_text)
    print(
        tabulate(
            table,
            headers=["Number", "Amount", "Interest", "Principal", "Balance"],
            floatfmt=",.2f",
            numalign="right",
        )
    )


if __name__ == "__main__":
    main()
