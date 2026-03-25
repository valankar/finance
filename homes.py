#!/usr/bin/env python3
"""Get estimated home values."""

import typing
from datetime import datetime

import pandas as pd
from cyclopts import App, Parameter, Token
from dateutil import parser
from loguru import logger
from pydantic import BaseModel

import common


class Property(typing.NamedTuple):
    name: str
    redfin_url: str
    zillow_url: str
    address: str


PROPERTIES = ()


def _pivot_resample(df: pd.DataFrame, suffix: str) -> pd.DataFrame:
    """Pivot dataframe and resample daily with interpolation."""
    return (
        df.drop(columns="site")
        .pivot_table(index="date", columns="name", values="value")
        .resample("D")
        .last()
        .interpolate()
        .add_suffix(suffix)
    )


def get_real_estate_df() -> pd.DataFrame:
    """Get combined real estate prices and rents."""
    df = common.read_sql_table("real_estate_prices")

    # Process each site type with appropriate suffixes
    redfin = _pivot_resample(df.query("site == 'Redfin'"), " Redfin")
    zillow = _pivot_resample(df.query("site == 'Zillow'"), " Zillow")
    taxes = _pivot_resample(df.query("site == 'Taxes'"), " Taxes")

    # Merge all price sources and calculate means for each property
    merged = common.reduce_merge_asof([redfin, zillow, taxes]).interpolate()
    means = {
        p.name: merged[
            [f"{p.name} Redfin", f"{p.name} Zillow", f"{p.name} Taxes"]
        ].mean(axis=1)
        for p in PROPERTIES
    }
    price_df = pd.DataFrame(means).add_suffix(" Price")

    # Get rent data
    rent_df = _pivot_resample(common.read_sql_table("real_estate_rents"), " Rent")

    # Merge prices and rents, apply 30-day rolling mean
    return (
        common.reduce_merge_asof([price_df, rent_df])
        .interpolate()
        .sort_index(axis=1)
        .rolling("30D")
        .mean()
    )


def get_property(name: str) -> Property | None:
    for p in PROPERTIES:
        if p.name == name:
            return p
    return None


def write_prices_table(name, value, site, timestamp: typing.Optional[datetime] = None):
    """Write prices to sql."""
    common.insert_sql(
        "real_estate_prices", {"name": name, "value": value, "site": site}, timestamp
    )


def write_rents_table(name, value, site):
    """Write rents to sql."""
    common.insert_sql("real_estate_rents", {"name": name, "value": value, "site": site})


app = App()


def comma_separated(type_, tokens: typing.Sequence[Token]) -> int:
    return type_(tokens[0].value.replace(",", ""))


type CommaInt = typing.Annotated[int, Parameter(converter=comma_separated)]


class RedfinEstimate(BaseModel):
    value: int


class ZillowEstimate(BaseModel):
    zestimate: int
    estimated_rent: int


async def get_from_browser_use():
    for p in PROPERTIES:
        redfin_price = zillow_price = zillow_rent = None
        redfin_t = await common.run_browser_use(
            task=f"Get the estimate value from https://www.redfin.com{p.redfin_url}. Do not go to any other site.",
            model=RedfinEstimate,
        )
        zillow_t = await common.run_browser_use(
            task=f"Get the zestimate and estimated rent from https://www.zillow.com{p.zillow_url}. Do not go to any other site.",
            model=ZillowEstimate,
        )
        if o := redfin_t.output:
            redfin_price = o.value
        if o := zillow_t.output:
            zillow_price = o.zestimate
            zillow_rent = o.estimated_rent
        if all((redfin_price, zillow_price, zillow_rent)):
            logger.info(
                f"Name: {p.name} Redfin: {redfin_price} Zillow: {zillow_price} Zillow Rent: {zillow_rent}"
            )
            write_prices_table(p.name, zillow_price, "Zillow")
            write_rents_table(p.name, zillow_rent, "Zillow")
            write_prices_table(p.name, redfin_price, "Redfin")


@app.default
async def main(
    name: typing.Optional[str] = None,
    redfin_price: typing.Optional[CommaInt] = None,
    zillow_price: typing.Optional[CommaInt] = None,
    zillow_rent: typing.Optional[CommaInt] = None,
    taxes_price: typing.Optional[CommaInt] = None,
    date: typing.Optional[str] = None,
):
    """Main."""
    if name:
        if p := get_property(name):
            timestamp = None
            if date:
                timestamp = parser.parse(date)
            for arg, site in (
                (redfin_price, "Redfin"),
                (zillow_price, "Zillow"),
                (taxes_price, "Taxes"),
            ):
                if arg:
                    write_prices_table(p.name, arg, site, timestamp)
            if zillow_rent:
                write_rents_table(p.name, zillow_rent, "Zillow")
        else:
            print(f"Property {name} is unknown")
    else:
        await get_from_browser_use()


if __name__ == "__main__":
    app()
