#!/usr/bin/env python3
"""Store IBKR margin rate history for USD, CHF."""

import pandas as pd

import common


def get_interest_rates(page):
    """Get interest rate list as [USD, CHF]."""
    page.get_by_role("link", name=" Accept Cookies").click()

    def get_percent(currency):
        return float(
            page.get_by_role("row", name=currency)
            .get_by_role("cell")
            .nth(2)
            .inner_text()
            .split()[0]
            .strip("%")
        )

    return [get_percent("USD"), get_percent("CHF")]


def main():
    """Writes IB margin rates to DB."""
    with common.run_with_browser_page(
        "https://www.interactivebrokers.com/en/trading/margin-rates.php"
    ) as page:
        usd, chf = get_interest_rates(page)
    new_df = pd.DataFrame({"USD": usd, "CHF": chf}, index=[pd.Timestamp.now()])
    common.to_sql(new_df, "interactive_brokers_margin_rates")


if __name__ == "__main__":
    main()
