#!/usr/bin/env python3
"""Store IBKR margin rate history for USD, CHF."""

import re

from loguru import logger

import common


def get_interest_rate(currency):
    """Get interest rate from IB."""
    logger.info(f"Getting interest rate for {currency=}")
    with common.run_with_browser_page(
        "https://www.interactivebrokers.com/en/trading/margin-rates.php"
    ) as page:
        page.get_by_text("Stay on US website").click()
        page.get_by_role("link", name="Accept Cookies").click()
        row = page.locator("table tr", has=page.locator("td", has_text=f"{currency}"))
        rate = row.locator("td", has_text="%").first.text_content()
        rate = re.sub(r"%.*", "", rate)  # type: ignore
        return float(rate)


def main():
    """Writes IB margin rates to DB."""
    interest_rates = {}
    for currency in ["USD", "CHF"]:
        interest_rates[currency] = get_interest_rate(currency)
    common.insert_sql("interactive_brokers_margin_rates", interest_rates)


if __name__ == "__main__":
    main()
