#!/usr/bin/env python3
"""Store Wealthfront Cash yield history."""

from datetime import date
import pandas as pd

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver import FirefoxOptions
from selenium.webdriver.common.by import By

import common

WEALTHFRONT_CASH_CSV = common.PREFIX + 'wealthfront_cash_yield.csv'


def get_yield():
    """Get yield from Wealthfront support page with Firefox/Selenium."""
    opts = FirefoxOptions()
    opts.add_argument("--headless")
    browser = webdriver.Firefox(options=opts)
    soup = None
    try:
        browser.get(
            # pylint: disable-next=line-too-long
            'https://support.wealthfront.com/hc/en-us/articles/360043680212-Interest-rate-for-Cash-Accounts'
        )
        elem = browser.find_element(By.CLASS_NAME, 'content-body')
        soup = BeautifulSoup(elem.get_attribute('innerHTML'), 'html.parser')
    finally:
        browser.quit()
    # This gets the APY.
    return float(soup.find_all('strong')[0].text)


def main():
    """Writes 7 day yield history to CSV file."""
    new_df = pd.DataFrame({'percent': get_yield()}, index=[date.today()])
    new_df.to_csv(WEALTHFRONT_CASH_CSV,
                  mode='a',
                  header=False,
                  float_format='%.2f')


if __name__ == '__main__':
    main()
