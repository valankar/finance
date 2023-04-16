#!/usr/bin/env python3
"""Determine price for Vanguard trust."""

import sys

from selenium.webdriver.common.by import By

import common


def get_price_browser():
    """Get Vanguard trust price with Selenium."""
    browser = common.get_browser()
    browser.get(
        'https://investor.vanguard.com/mutual-funds/profile/pe/overview/7741')
    price = float(
        browser.find_element(
            By.XPATH,
            # pylint: disable-next=line-too-long
            '//*[@id="ng-app-fundprofile"]/div/div[2]/div[2]/div[3]/div/div/div[2]/div[4]/div[1]/table/tbody/tr[1]/td[2]'
        ).get_attribute('innerHTML').strip('$'))
    return price


def main():
    """Main."""
    price = get_price_browser()
    if not price:
        sys.exit('Failed to get Vanguard Target Retirement 2040 Trust price.')
    output = common.PREFIX + 'vanguard.txt'
    with common.temporary_file_move(output) as output_file:
        output_file.write(str(price))


if __name__ == '__main__':
    main()
