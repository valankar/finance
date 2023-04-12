#!/usr/bin/env python3
"""Store Wealthfront Cash yield history."""

from datetime import date, datetime
import pandas as pd

from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By

import common

WEALTHFRONT_CASH_CSV = common.PREFIX + 'wealthfront_cash_yield.csv'
# End date, how much to boost.
APY_BOOST = ('2023-07-09', 0.50)


def get_yield():
    """Get yield from Wealthfront support page with Firefox/Selenium."""
    browser = common.get_browser()
    browser.get(
        # pylint: disable-next=line-too-long
        'https://support.wealthfront.com/hc/en-us/articles/360043680212-Interest-rate-for-Cash-Accounts'
    )
    elem = browser.find_element(By.CLASS_NAME, 'content-body')
    soup = BeautifulSoup(elem.get_attribute('innerHTML'), 'html.parser')
    # This gets the APY.
    return float(soup.find_all('strong')[0].text)


def main():
    """Writes 7 day yield history to CSV file."""
    today = date.today()
    boost = 0
    if today <= datetime.strptime(APY_BOOST[0], '%Y-%m-%d').date():
        boost = APY_BOOST[1]
    new_df = pd.DataFrame({'percent': get_yield() + boost}, index=[today])
    new_df.to_csv(WEALTHFRONT_CASH_CSV,
                  mode='a',
                  header=False,
                  float_format='%.2f')


if __name__ == '__main__':
    main()
