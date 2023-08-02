#!/usr/bin/env python3
"""Get Schwab IRA value."""

from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait

import common

TABLE_PREFIX = "schwab_ira"
CSV_OUTPUT_PATH = f"{common.PREFIX}schwab_ira_values.csv"


def browser_execute_before(browser):
    """Click popup that sometimes appears."""
    try:
        WebDriverWait(browser, timeout=30).until(
            lambda d: d.find_element(By.PARTIAL_LINK_TEXT, "Continue with a limited")
        ).click()
    except TimeoutException:
        pass


def get_ticker():
    """Get ticker price from Schwab via Selenium."""
    return float(
        common.find_xpath_via_browser(
            "https://www.schwabassetmanagement.com/products/stir?product=swygx",
            '//*[@id="sfm-table--fundprofile"]/table/tbody/tr[5]/td[2]',
            execute_before=browser_execute_before,
        ).strip("$")
    )


def main():
    """Main."""
    common.write_ticker_csv(
        f"{TABLE_PREFIX}_amounts",
        f"{TABLE_PREFIX}_prices",
        CSV_OUTPUT_PATH,
        ticker_prices={"SWYGX": get_ticker()},
    )


if __name__ == "__main__":
    main()
