#!/usr/bin/env python3
"""Get estimated home values."""

import statistics

import pandas as pd

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

# pylint: disable=line-too-long


def integerize_value(value):
    """Removes $ and , from value."""
    return int(value.translate(str.maketrans("", "", "$,")))


def get_redfin_estimate(url_path):
    """Get home value from Redfin."""
    return integerize_value(
        common.find_xpath_via_browser(
            f"https://www.redfin.com{url_path}",
            '//*[@id="content"]/div[12]/div[2]/div[1]/div/div/div/div[1]/div[2]/div/div/div/div/div[1]/div/span',
        )
    )


def get_zillow_estimates(url_path):
    """Get home and rent value from Zillow."""
    values = common.find_xpaths_via_browser(
        f"https://www.zillow.com{url_path}",
        [
            '//*[@id="search-detail-lightbox"]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div/div[1]/span/span[2]/span/span',
            '//*[@id="search-detail-lightbox"]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div/div[1]/span/span[4]/span/span',
        ],
    )
    return [integerize_value(x) for x in values]


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
        zillow, zillow_rent = get_zillow_estimates(ZILLOW_URLS[filename])
        average = round(statistics.mean([redfin, zillow]))
        if not average:
            continue
        with common.temporary_file_move(f"{common.PREFIX}{filename}") as output_file:
            output_file.write(str(average))
        write_prices_table(FILE_TO_NAME[filename], redfin, zillow)

        # Home rent estimate
        write_rents_table(
            FILE_TO_NAME[filename],
            zillow_rent,
        )


if __name__ == "__main__":
    main()
