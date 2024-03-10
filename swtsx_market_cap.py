#!/usr/bin/env python3
"""Get market caps of SWTSX."""

import pandas as pd

from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait

import common
import schwab_ira


def browser_execute_before(browser):
    """Click into portfolio section to get market cap weightings."""
    # Click popup that sometimes appears
    schwab_ira.browser_execute_before(browser)
    try:
        WebDriverWait(browser, timeout=30).until(
            lambda d: d.find_element(By.XPATH, '//*[@id="portfolio"]/h2')
        ).click()
    except TimeoutException:
        pass


def reduce_sum_percents(string_values):
    """Sum values in list of percentage strings."""
    return sum(map(lambda x: float(x.strip("%")), string_values))


def save_market_cap():
    """Writes SWTSX market cap weightings to swtsx_market_cap DB table."""
    table_list = common.find_xpath_via_browser(
        "https://www.schwabassetmanagement.com/products/swtsx",
        '//*[@id="marketcap"]',
        execute_before=browser_execute_before,
    ).split("\n")[-10:]
    # Example:
    # pylint: disable-next=line-too-long
    # ['<$1,000 M', '0.87%', '$1,000-$3,000 M', '2.15%', '$3,000-$15,000 M', '10.10%', '$15,000-$70,000 M', '22.00%', '> $70,000 M', '64.88%']
    market_cap_dict = {}
    market_cap_dict["US_LARGE_CAP"] = reduce_sum_percents(
        [table_list[-1], table_list[-3]]
    )
    market_cap_dict["US_SMALL_CAP"] = reduce_sum_percents(
        [table_list[-5], table_list[-7], table_list[-9]]
    )
    market_cap_df = pd.DataFrame(
        market_cap_dict,
        index=[pd.Timestamp.now()],
    )
    common.to_sql(market_cap_df, "swtsx_market_cap")


def main():
    """Main."""
    save_market_cap()


if __name__ == "__main__":
    main()
