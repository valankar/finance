#!/usr/bin/env python3
"""Calculate the maximum balance on pledged asset line given a monthly payment."""

import argparse
import io
import subprocess

from amortization.schedule import amortization_schedule
import numpy_financial
from tabulate import tabulate

import common

# Desired monthly payment
MONTHLY_PAYMENT = 800

# APY is SOFR plus this amount
APY_OVER_SOFR = 2.9

# How long to amortize over.
MONTHS = 12

# Ledger command to get monthly investment income.
# pylint: disable-next=line-too-long
LEDGER_INCOME_CMD = f"{common.LEDGER_PREFIX} -D -n -A -b '2023' --tail 1 -J register \\(^Income:Dividends or ^Income:Interest\\)"


def get_args():
    """Get command line arguments."""
    parser = argparse.ArgumentParser(
        prog="amortize_pal",
        description="Determine maximum PAL balance given monthly payments.",
    )
    parser.add_argument("--apy_over_sofr", default=APY_OVER_SOFR, type=float)
    parser.add_argument(
        "--monthly_payment", default=get_monthly_investment_income(), type=int
    )
    parser.add_argument("--months", default=MONTHS, type=int)
    return parser.parse_args()


def get_monthly_investment_income():
    """Get monthly dividend and interest income."""
    return round(
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


def main():
    """Main."""
    args = get_args()
    sofr = common.read_sql_table("sofr").iloc[-1]["percent"]
    apy = (sofr + args.apy_over_sofr) / 100
    amount = round(
        abs(numpy_financial.pv(apy / args.months, args.months, args.monthly_payment)), 2
    )
    table = (x for x in amortization_schedule(amount, apy, args.months))
    print(
        f"Loan of ${amount:,} would result in {args.months} monthly payments "
        f"of ${args.monthly_payment:,} at APY {apy*100:.2f}% ({sofr} (SOFR) "
        f"+ {args.apy_over_sofr})"
    )
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
