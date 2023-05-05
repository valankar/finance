#!/usr/bin/env python3
"""Get estimated home values."""

import shutil
from datetime import datetime, date
from pathlib import Path

import numpy as np
import pandas as pd

import common

REDFIN_URLS = {
    "mtvernon.txt": "/CA/Mountain-View/1935-Mount-Vernon-Ct-94040/unit-1/home/1186556",
    "northlake.txt": "/CA/San-Jose/401-Northlake-Dr-95117/unit-32/home/752126",
    "villamaria.txt": "/CA/San-Jose/1048-Villa-Maria-Ct-95125/home/844776",
}

ZILLOW_URLS = {
    "mtvernon.txt": "/homedetails/1935-Mount-Vernon-Ct-APT-1-Mountain-View-CA-94040/19514058_zpid/",
    "northlake.txt": "/homedetails/401-Northlake-Dr-APT-32-San-Jose-CA-95117/19608577_zpid/",
    "villamaria.txt": "/homedetails/1048-Villa-Maria-Ct-San-Jose-CA-95125/19684489_zpid/",
}

PURCHASE_PRICES = {
    "mtvernon.txt": ("2012-07-09", 438000),
    "northlake.txt": ("2014-04-16", 532500),
    "villamaria.txt": ("2013-04-15", 475000),
}

HOME_COLUMN_MAP = {
    "californiast.txt": "California St",
    "corallake.txt": "Coral Lake",
    "mtvernon.txt": "Mt Vernon",
    "northlake.txt": "Northlake",
    "villamaria.txt": "Villa Maria",
}

# Where to write home csv files.
OUTPUT_DIR = f"{Path.home()}/bin/accounts/historical/"

# If there is a new value difference of this much, repopulate all old data.
# See https://twitter.com/valankar/status/1586667545486057472.
HISTORICAL_MERGE_THRESHOLD = 1000


def get_home_estimate(filename):
    """Get Redfin and Zillow average price."""
    return get_redfin_estimate(REDFIN_URLS[filename])
    # Requires captcha.
    # return round(
    #     statistics.mean(
    #         [
    #             get_redfin_estimate(REDFIN_URLS[filename]),
    #             get_zillow_estimate(ZILLOW_URLS[filename]),
    #         ]
    #     )
    # )


def get_redfin_estimate(url_path):
    """Get home value from Redfin."""
    return int(
        common.find_xpath_via_browser(
            f"https://www.redfin.com{url_path}",
            # pylint: disable-next=line-too-long
            '//*[@id="content"]/div[12]/div[2]/div[1]/div/div[1]/div/div[1]/div/div/div/div/div/div[1]/div/span',
        ).translate(str.maketrans("", "", "$,"))
    )


def get_zillow_estimate(url_path):
    """Get home value from Zillow."""
    return int(
        common.find_xpath_via_browser(
            f"https://www.zillow.com{url_path}",
            '//*[@id="home-details-home-values"]/div/div[1]/div/div/div[1]/div/p/h3',
        ).translate(str.maketrans("", "", "$,"))
    )


def create_csv():
    """Create CSV files of homes with purchase and current price."""
    today = date.today()
    for filename in REDFIN_URLS:
        current_price = get_home_estimate(filename)
        purchase_date = datetime.strptime(
            PURCHASE_PRICES[filename][0], "%Y-%m-%d"
        ).date()
        purchase_price = PURCHASE_PRICES[filename][1]
        home_df = pd.DataFrame(
            {"value": [purchase_price, current_price]}, index=[purchase_date, today]
        ).rename_axis("date")
        home_df.to_csv(f"{OUTPUT_DIR}{filename}.csv")


def merge_home_data():
    """Merge home data into account history."""
    accounts_df = pd.read_csv(
        f"{common.PREFIX}account_history.csv",
        index_col=0,
        parse_dates=True,
        infer_datetime_format=True,
        header=[0, 1, 2, 3],
    )
    for home, column in HOME_COLUMN_MAP.items():
        home_df = pd.read_csv(
            f"{OUTPUT_DIR}{home}.csv",
            index_col=0,
            parse_dates=True,
            infer_datetime_format=True,
        )
        home_df = home_df.resample("D").mean().interpolate()
        column = ("USD", "Real Estate", column, "nan")
        accounts_df[column] = np.nan
        new_df = pd.merge_asof(
            accounts_df[[column]].droplevel([1, 2, 3], axis=1),
            home_df,
            left_index=True,
            right_index=True,
            tolerance=pd.Timedelta("1d"),
        )
        accounts_df[column] = new_df["value"]
    # Sales
    accounts_df.loc["2019-10-16":, ("USD", "Real Estate", "Coral Lake", "nan")] = 0
    accounts_df.loc["2014-03-01":, ("USD", "Real Estate", "California St", "nan")] = 0
    accounts_df = accounts_df.sort_index().interpolate()

    shutil.copy(
        f"{common.PREFIX}account_history.csv", f"{common.PREFIX}account_history.csv.old"
    )
    with common.temporary_file_move(
        f"{common.PREFIX}account_history.csv"
    ) as output_file:
        accounts_df.to_csv(output_file, float_format="%.2f")
    all_df = pd.read_csv(
        f"{common.PREFIX}history.csv",
        index_col=0,
        parse_dates=True,
        infer_datetime_format=True,
    )
    sum_df = accounts_df["USD"]["Real Estate"].sum(axis=1)
    sum_df.name = "new_total_real_estate"
    all_merged = pd.merge_asof(all_df, sum_df, left_index=True, right_index=True)
    all_merged["total_real_estate"] = all_merged["new_total_real_estate"]
    all_merged = all_merged.drop(columns="new_total_real_estate")

    all_merged["total"] = (
        all_merged["total_investing"]
        + all_merged["total_retirement"]
        + all_merged["total_real_estate"]
        + all_merged["total_liquid"]
    )
    all_merged["total_no_homes"] = (
        all_merged["total_investing"]
        + all_merged["total_retirement"]
        + all_merged["total_liquid"]
    )
    shutil.copy(f"{common.PREFIX}history.csv", f"{common.PREFIX}history.csv.old")
    with common.temporary_file_move(f"{common.PREFIX}history.csv") as output_file:
        all_merged.to_csv(output_file, float_format="%.2f")


def main():
    """Main."""
    historical_merge_required = False
    for filename in REDFIN_URLS:
        value = get_home_estimate(filename)
        output = common.PREFIX + filename
        if not value:
            continue
        with open(output, encoding="utf-8") as input_file:
            old_value = float(input_file.read())
            if abs(value - old_value) > HISTORICAL_MERGE_THRESHOLD:
                print(f"{filename} old: {old_value} new: {value}")
                historical_merge_required = True
        with common.temporary_file_move(output) as output_file:
            output_file.write(str(value))

    if historical_merge_required:
        print("Home price data differs by threshold, doing historical merge.")
        create_csv()
        merge_home_data()


if __name__ == "__main__":
    main()
