#!/usr/bin/env python3
"""Determine price for Vanguard trust."""

import sys

import common


def get_price_browser():
    """Get Vanguard trust price with Selenium."""
    return float(
        common.find_xpath_via_browser(
            'https://investor.vanguard.com/mutual-funds/profile/pe/overview/7741',
            # pylint: disable-next=line-too-long
            '//*[@id="ng-app-fundprofile"]/div/div[2]/div[2]/div[3]/div/div/div[2]/div[4]/div[1]/table/tbody/tr[1]/td[2]'
        ).strip('$'))


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
