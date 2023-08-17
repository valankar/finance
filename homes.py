#!/usr/bin/env python3
"""Get estimated home values."""

import statistics

import pandas as pd
from retry import retry

import common

REDFIN_URLS = {
    "prop1.txt": "/some/redfin_url",
}

ZILLOW_URLS = {
    "prop1.txt": "/some/zillow_url",
}

FILE_TO_NAME = {
    "prop1.txt": "Property Name",
}


# Retry a few times to avoid captcha.
@retry(delay=30, tries=4)
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
        '//*[@id="content"]/div[11]/div[2]/div[1]/div/div/div/div[1]/div[2]/div/div/div/div/div[1]/div/span',
    )


def get_zillow_estimate(url_path):
    """Get home value from Zillow."""
    return get_site_estimate(
        f"https://www.zillow.com{url_path}",
        '//*[@id="home-details-home-values"]/div/div[1]/div/div/div[1]/div/p/h3',
    )


def get_zillow_rent_estimate(url_path):
    """Get rent estimate from Zillow."""
    return get_site_estimate(
        f"https://www.zillow.com{url_path}",
        '//*[@id="__next"]/div/div/div[1]/div[2]/div[2]/div[2]/div/div[1]/span/span[4]/span/span',
    )


def write_prices_table(name, redfin, zillow):
    """Write prices to sqlite."""
    home_df = pd.DataFrame(
        {"name": name, "redfin_value": redfin, "zillow_value": zillow},
        index=[pd.Timestamp.now()],
    )
    common.to_sql(home_df, "real_estate_prices", foreign_key=True)


def write_rents_table(name, value):
    """Write rents to sqlite."""
    home_df = pd.DataFrame({"name": name, "value": value}, index=[pd.Timestamp.now()])
    common.to_sql(home_df, "real_estate_rents", foreign_key=True)


def main():
    """Main."""
    for filename, redfin_url in REDFIN_URLS.items():
        # Home value
        redfin = get_redfin_estimate(redfin_url)
        zillow = get_zillow_estimate(ZILLOW_URLS[filename])
        average = round(statistics.mean([redfin, zillow]))
        if not average:
            continue
        with common.temporary_file_move(f"{common.PREFIX}{filename}") as output_file:
            output_file.write(str(average))
        write_prices_table(FILE_TO_NAME[filename], redfin, zillow)

        # Home rent estimate
        write_rents_table(
            FILE_TO_NAME[filename],
            get_zillow_rent_estimate(ZILLOW_URLS[filename]),
        )


if __name__ == "__main__":
    main()
