#!/usr/bin/env python3
"""Get market caps of SWTSX."""

import pandas as pd

from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

import common


def browser_execute_before(browser):
    """Click into portfolio section to get market cap weightings."""
    # Click popup that sometimes appears
    common.schwab_browser_execute_before(browser)
    try:
        WebDriverWait(browser, timeout=30).until(
            EC.element_to_be_clickable((By.XPATH, '//*[@id="portfolio"]/h2'))
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
    ).split("\n")
    # Example:
    # pylint: disable=line-too-long
    # ['Market Cap', '03/31/2024', 'Market Cap Percent of Portfolio (%)', '<$1,000 M', '0.78%', '$1,000-$3,000 M', '1.95%', '$3,000-$15,000 M', '9.15%', '$15,000-$70,000 M', '20.83%', '> $70,000 M', '67.28%', '[N/A]', '0.01%']
    table_dict = dict(zip(table_list[1::2], table_list[2::2]))
    # {'03/31/2024': 'Market Cap Percent of Portfolio (%)', '<$1,000 M': '0.78%', '$1,000-$3,000 M': '1.95%', '$3,000-$15,000 M': '9.15%', '$15,000-$70,000 M': '20.83%', '> $70,000 M': '67.28%', '[N/A]': '0.01%'}
    market_cap_dict = {}
    # For determining large vs small cap, compare:
    # https://www.schwabassetmanagement.com/products/scha
    # https://www.schwabassetmanagement.com/products/swtsx
    market_cap_dict["US_LARGE_CAP"] = reduce_sum_percents(
        [table_dict["> $70,000 M"], table_dict["$15,000-$70,000 M"]]
    )
    market_cap_dict["US_SMALL_CAP"] = reduce_sum_percents(
        [
            table_dict["<$1,000 M"],
            table_dict["$1,000-$3,000 M"],
            table_dict["$3,000-$15,000 M"],
        ]
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
