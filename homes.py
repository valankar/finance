#!/usr/bin/env python3
"""Get estimated home values."""

import functools
import statistics
from datetime import date
from pathlib import Path

import pandas as pd

import common

REDFIN_URLS = {
    "prop1.txt": "/some/redfin_url",
}

ZILLOW_URLS = {
    "prop1.txt": "/some/zillow_url",
}

PURCHASE_PRICES = {
    "prop1.txt": ("2012-07-09", 438000),
}

HOME_COLUMN_MAP = {
    "prop1.txt": "Some Property",
}

# Where to write home csv files.
OUTPUT_DIR = f"{Path.home()}/bin/accounts/historical/"


@functools.cache
def get_home_estimate(filename):
    """Get home average price."""
    return round(
        statistics.mean(
            [
                get_redfin_estimate(REDFIN_URLS[filename]),
                get_zillow_estimate(ZILLOW_URLS[filename]),
            ]
        )
    )


def get_site_estimate(url, xpath):
    """Get home value from either Redfin or Zillow."""
    return int(
        common.find_xpath_via_browser(
            url,
            xpath,
        ).translate(str.maketrans("", "", "$,"))
    )


def get_redfin_estimate(url_path):
    """Get home value from Redfin."""
    return get_site_estimate(
        f"https://www.redfin.com{url_path}",
        # pylint: disable-next=line-too-long
        '//*[@id="content"]/div[12]/div[2]/div[1]/div/div[1]/div/div[1]/div/div/div/div/div/div[1]/div/span',
    )


def get_zillow_estimate(url_path):
    """Get home value from Zillow."""
    return get_site_estimate(
        f"https://www.zillow.com{url_path}",
        '//*[@id="home-details-home-values"]/div/div[1]/div/div/div[1]/div/p/h3',
    )


def main():
    """Main."""
    for filename in REDFIN_URLS:
        value = get_home_estimate(filename)
        output = common.PREFIX + filename
        if not value:
            continue
        with common.temporary_file_move(output) as output_file:
            output_file.write(str(value))
        home_df = pd.DataFrame({"value": value}, index=[date.today()])
        home_df.to_csv(f"{OUTPUT_DIR}{filename}.csv", mode="a", header=False)


if __name__ == "__main__":
    main()
