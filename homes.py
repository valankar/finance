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


def write_table(table, value):
    """Write table to sqlite."""
    home_df = pd.DataFrame({"value": value}, index=[pd.Timestamp.now()])
    common.to_sql(home_df, table)


def main():
    """Main."""
    for filename in REDFIN_URLS:
        # Home value
        value = get_home_estimate(filename)
        output = common.PREFIX + filename
        if not value:
            continue
        with common.temporary_file_move(output) as output_file:
            output_file.write(str(value))
        table_name = filename.split(".")[0]
        write_table(table_name, value)

        # Home rent estimate
        write_table(
            f"{table_name}_rent",
            get_zillow_rent_estimate(ZILLOW_URLS[filename]),
        )


if __name__ == "__main__":
    main()
