#!/usr/bin/env python3
"""Get estimated home values."""

import argparse
import re
import typing

import pandas as pd
from loguru import logger
from playwright.sync_api import TimeoutError

import common
import resample_table


class Property(typing.NamedTuple):
    name: str
    redfin_url: str
    zillow_url: str
    address: str


PROPERTIES = (
    Property(
        name="Some Real Estate",
        redfin_url="URL",
        zillow_url="URL",
        address="ADDRESS",
    ),
)


def get_real_estate_df() -> pd.DataFrame:
    df = common.read_sql_table("real_estate_prices")
    redfin = (
        df.query("site == 'Redfin'")
        .drop(columns="site")
        .pivot_table(index="date", columns="name", values="value")
        .resample("D")
        .last()
        .interpolate()
    )
    zillow = (
        df.query("site == 'Zillow'")
        .drop(columns="site")
        .pivot_table(index="date", columns="name", values="value")
        .resample("D")
        .last()
        .interpolate()
    )
    taxes = (
        df.query("site == 'Taxes'")
        .drop(columns="site")
        .pivot_table(index="date", columns="name", values="value")
        .add_suffix(" Taxes")
        .resample("D")
        .last()
        .interpolate()
    )
    merged = pd.merge_asof(
        redfin,
        zillow,
        left_index=True,
        right_index=True,
        suffixes=(" Redfin", " Zillow"),
    )
    merged = pd.merge_asof(
        merged,
        taxes,
        left_index=True,
        right_index=True,
    ).interpolate()
    for p in PROPERTIES:
        merged[p.name] = merged[
            [f"{p.name} Redfin", f"{p.name} Zillow", f"{p.name} Taxes"]
        ].mean(axis=1)
    price_df = merged[[p.name for p in PROPERTIES]].add_suffix(" Price")
    rent_df = (
        common.read_sql_table("real_estate_rents")
        .pivot_table(index="date", columns="name", values="value")
        .add_suffix(" Rent")
    )
    return (
        common.reduce_merge_asof([price_df, rent_df]).interpolate().sort_index(axis=1)
    )


def recalculate_history():
    re_df = get_real_estate_df()
    hist = common.read_sql_table("history")
    sum_re = re_df[[f"{p.name} Price" for p in PROPERTIES]].sum(axis=1)
    sum_re.name = "re_new"
    new_hist = pd.merge_asof(hist, sum_re, left_index=True, right_index=True)
    new_hist["total_real_estate"] = new_hist["re_new"]
    new_hist = new_hist.drop(
        columns=resample_table.TABLES_DROP_COLUMNS["history"] + ["re_new"]
    )
    resample_table.rewrite_table("history", new_hist)


def get_property(name: str) -> Property | None:
    for p in PROPERTIES:
        if p.name == name:
            return p
    return None


def integerize_value(value):
    return int(re.sub(r"\D", "", value))


def get_redfin_estimate(url_path):
    """Get home value from Redfin."""
    logger.info(f"Getting Redfin estimate for {url_path}")
    with common.run_with_browser_page(f"https://www.redfin.com{url_path}") as page:
        return integerize_value(
            page.get_by_text(re.compile(r"^\$[\d,]+$")).all()[0].inner_text()
        )


def get_zillow_estimates(url_path):
    """Get home and rent value from Zillow."""
    logger.info(f"Getting Zillow estimate for {url_path}")
    with common.run_with_browser_page(f"https://www.zillow.com{url_path}") as page:
        try:
            price = page.get_by_test_id("price").get_by_text("$").inner_text()
            rent = page.get_by_test_id("rent-zestimate").inner_text()
        except TimeoutError:
            logger.info("Got timeout error, trying with different matching")
            if matches := re.search(
                r"Zestimate.+: \$([\d,]+).*Rent Zestimate.+: \$([\d,]+)",
                page.get_by_test_id("summary").inner_text(),
            ):
                price = matches[1]
                rent = matches[2]
        return [integerize_value(price), integerize_value(rent)]


def write_prices_table(
    name, value, site, timestamp: typing.Optional[pd.Timestamp] = None
):
    """Write prices to sqlite."""
    if timestamp is None:
        timestamp = pd.Timestamp.now()
    home_df = pd.DataFrame(
        {"name": name, "value": value, "site": site},
        index=[timestamp],
    )
    common.to_sql(home_df, "real_estate_prices")


def write_rents_table(name, value, site):
    """Write rents to sqlite."""
    home_df = pd.DataFrame(
        {"name": name, "value": value, "site": site}, index=[pd.Timestamp.now()]
    )
    common.to_sql(home_df, "real_estate_rents")


def process_redfin(p: Property):
    redfin = get_redfin_estimate(p.redfin_url)
    write_prices_table(p.name, redfin, "Redfin")


def process_zillow(p: Property):
    zillow, zillow_rent = get_zillow_estimates(p.zillow_url)
    write_prices_table(p.name, zillow, "Zillow")
    write_rents_table(p.name, zillow_rent, "Zillow")


def process_home(p: Property):
    process_redfin(p)
    process_zillow(p)


def main():
    """Main."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", required=False)
    parser.add_argument("--redfin-price", required=False, type=int)
    parser.add_argument("--zillow-price", required=False, type=int)
    parser.add_argument("--zillow-rent", required=False, type=int)
    parser.add_argument("--taxes-price", required=False, type=int)
    parser.add_argument("--date", required=False, type=str)
    parser.add_argument(
        "--recalculate-history", default=False, action=argparse.BooleanOptionalAction
    )
    parser.add_argument(
        "--process-home", default=False, action=argparse.BooleanOptionalAction
    )
    args = parser.parse_args()
    if args.recalculate_history:
        return recalculate_history()
    if p := get_property(args.name):
        if args.process_home:
            return process_home(p)
        timestamp = None
        if args.date:
            timestamp = pd.Timestamp(args.date)
        for arg, site in (
            (args.redfin_price, "Redfin"),
            (args.zillow_price, "Zillow"),
            (args.taxes_price, "Taxes"),
        ):
            if arg:
                write_prices_table(p.name, arg, site, timestamp)
        if args.zillow_rent:
            write_rents_table(p.name, args.zillow_rent, "Zillow")
    else:
        print(f"Property {args.name} is unknown")


if __name__ == "__main__":
    main()
