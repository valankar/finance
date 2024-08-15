#!/usr/bin/env python3
"""Store IBKR margin rate history for USD, CHF."""

import subprocess

import pandas as pd

import common
import ledger_amounts


def get_ibkr_loan_balance(currency):
    """Get ledger balance for IB."""
    match currency:
        case "USD":
            ledger_currency = "\\\\$"
        case "CHF":
            ledger_currency = "CHF"
    value = int(
        float(
            subprocess.check_output(
                f"{ledger_amounts.LEDGER_BALANCE_CMD} --limit 'commodity=~/^{ledger_currency}$/' "
                "'Interactive Brokers'",
                text=True,
                shell=True,
            )
        )
    )
    if value > 0:
        return 1
    return abs(value)


def get_interest_rate(currency, loan):
    """Get interest rate from IB."""
    with common.run_with_browser_page(
        "https://www.interactivebrokers.com/en/trading/margin-rates.php"
    ) as page:
        page.get_by_role("link", name=" Accept Cookies").click()
        page.locator("#int_calc_db_balance").click()
        page.locator("#int_calc_db_balance").fill(f"{loan}")
        page.locator("#int_calc_db_currency").select_option(currency)
        page.get_by_role("link", name="Calculate Blended Rate", exact=True).click()
        page.get_by_role("heading", name="%").click()
        return float(page.get_by_role("heading", name="%").inner_text().strip("%"))


def main():
    """Writes IB margin rates to DB."""
    interest_rates = {}
    for currency in ["USD", "CHF"]:
        interest_rates[currency] = get_interest_rate(
            currency, get_ibkr_loan_balance(currency)
        )
    new_df = pd.DataFrame(interest_rates, index=[pd.Timestamp.now()])
    common.to_sql(new_df, "interactive_brokers_margin_rates")


if __name__ == "__main__":
    main()
