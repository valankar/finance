#!/usr/bin/env python3
"""Store Schwab SWVXX 7-day yield history."""

from datetime import date
import pandas as pd

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver import FirefoxOptions
from selenium.webdriver.common.by import By

import common

SWVXX_CSV = common.PREFIX + 'swvxx_yield.csv'


def get_yield():
    """Get 7 day yield with Firefox/Selenium."""
    opts = FirefoxOptions()
    opts.add_argument("--headless")
    browser = webdriver.Firefox(options=opts)
    soup = None
    try:
        browser.get('https://www.schwabassetmanagement.com/products/swvxx')
        elem = browser.find_element(By.ID, 'sfm-table--yields')
        soup = BeautifulSoup(elem.get_attribute('innerHTML'), 'html.parser')
    finally:
        browser.quit()
    # This gets the "7-Day Yield (with waivers)" value
    return float(soup.find_all('td')[1].text.strip('%'))


def write_yield():
    """Writes 7 day yield history to CSV file."""
    new_df = pd.DataFrame({'percent': get_yield()}, index=[date.today()])
    new_df.to_csv(SWVXX_CSV, mode='a', header=False, float_format='%.2f')


if __name__ == '__main__':
    write_yield()
