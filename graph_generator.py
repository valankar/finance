import typing
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Callable, Literal, Mapping, NamedTuple

import pandas as pd
import plotly.io as pio
from dateutil.relativedelta import relativedelta
from joblib import Memory, Parallel, delayed, parallel_config
from loguru import logger
from plotly.graph_objects import Figure

import brokerages
import common
import homes
import plot

type NonRangedGraphs = dict[str, dict]
type RangedGraphs = dict[str, dict[str, dict]]
type Graphs = dict[Literal["ranged", "nonranged"], NonRangedGraphs | RangedGraphs]


class GraphData(NamedTuple):
    graphs: Graphs
    last_updated_time: datetime
    last_generation_duration: timedelta
    latest_datapoint_time: pd.Timestamp


def get_xrange(
    dataframe: pd.DataFrame, selected_range: str
) -> tuple[str | datetime, str | datetime] | None:
    """Determine time range for selected button."""
    latest_time = dataframe.index[-1]
    earliest_time = dataframe.index[0]
    today_time = datetime.now()
    xrange = None
    relative = None
    match selected_range:
        case "All":
            xrange = (earliest_time, latest_time)
        case "3y":
            relative = relativedelta(years=-3)
        case "2y":
            relative = relativedelta(years=-2)
        case "1y":
            relative = relativedelta(years=-1)
        case "YTD":
            xrange = (today_time.strftime("%Y-01-01"), latest_time)
        case "6m":
            relative = relativedelta(months=-6)
        case "3m":
            relative = relativedelta(months=-3)
        case "1m":
            relative = relativedelta(months=-1)
        case "1d":
            relative = relativedelta(days=-1)
    if relative:
        xrange = ((latest_time + relative), latest_time)
    return xrange


def limit_and_resample_df(df: pd.DataFrame, selected_range: str) -> pd.DataFrame:
    """Limit df to selected range and resample."""
    if (retval := get_xrange(df, selected_range)) is None:
        return df
    start, end = retval
    df = df[start:end]
    match selected_range:
        case "1m" | "1d":
            window = None
        case "All" | "3y" | "2y":
            window = "W"
        case _:
            window = "D"
    if window:
        return df.resample(window).last().interpolate()
    return df


def get_plot_height_percent(name: str, layout: tuple[tuple[str, str], ...]) -> float:
    for n, height in layout:
        if n == name:
            return float(int(height[:-2]) / 100)
    return 1.0


def plot_generate(
    name: str, plot_func: Callable[[], Figure], layout: tuple[tuple[str, str], ...]
) -> tuple[str, dict]:
    pio.templates.default = common.PLOTLY_THEME
    fig = plot_func()
    write_image(fig, name, f"{common.PREFIX}/{name}.png", layout)
    return name, fig.to_plotly_json()


def plot_generate_ranged(
    name: str,
    plot_func: Callable[[str], Figure],
    r: str,
    layout: tuple[tuple[str, str], ...],
) -> tuple[str, dict]:
    pio.templates.default = common.PLOTLY_THEME
    fig = plot_func(r)
    write_image(fig, name, f"{common.PREFIX}/{name}-{r}.png", layout)
    return name, fig.to_plotly_json()


def write_image(fig: Figure, name: str, path: str, layout: tuple[tuple[str, str], ...]):
    fig.write_image(
        path,
        width=1024,
        height=768 * get_plot_height_percent(name, layout),
    )


@common.cache_forever_decorator
def generate_all_graphs(
    layout: tuple[tuple[str, str], ...],
    ranges: list[str],
    subplot_margin: dict[str, int],
) -> GraphData:
    """Generate and save all Plotly graphs."""
    logger.info("Generating graphs")
    start_time = datetime.now()
    dataframes = {
        "all": common.read_sql_table("history"),
        "brokerages": brokerages.load_df(),
        "real_estate": homes.get_real_estate_df(),
        "prices": common.read_sql_table("schwab_etfs_prices"),
        "forex": common.read_sql_table("forex"),
        "interest_rate": plot.get_interest_rate_df(),
    }
    nonranged_graphs_generate = [
        (
            "allocation_profit",
            lambda: plot.make_allocation_profit_section(
                dataframes["all"],
                dataframes["real_estate"],
            ).update_layout(margin=subplot_margin),
        ),
        (
            "change",
            lambda: plot.make_change_section(
                dataframes["all"],
                "total",
                "Total Net Worth Change",
            ),
        ),
        (
            "change_no_homes",
            lambda: plot.make_change_section(
                dataframes["all"],
                "total_no_homes",
                "Total Net Worth Change w/o Real Estate",
            ),
        ),
        (
            "investing_allocation",
            lambda: plot.make_investing_allocation_section(),
        ),
        (
            "loan",
            lambda: plot.make_loan_section().update_layout(margin=subplot_margin),
        ),
        (
            "daily_indicator",
            lambda: plot.make_daily_indicator(dataframes["all"]),
        ),
    ]
    ranged_graphs_generate = [
        (
            "assets_breakdown",
            lambda range: plot.make_assets_breakdown_section(
                limit_and_resample_df(dataframes["all"], range)
            ).update_layout(margin=subplot_margin),
        ),
        (
            "investing_retirement",
            lambda range: plot.make_investing_retirement_section(
                limit_and_resample_df(
                    dataframes["all"][["pillar2", "ira", "commodities", "etfs"]],
                    range,
                )
            ).update_layout(margin=subplot_margin),
        ),
        (
            "real_estate",
            lambda range: plot.make_real_estate_section(
                limit_and_resample_df(
                    dataframes["real_estate"],
                    range,
                )
            ).update_layout(margin=subplot_margin),
        ),
        (
            "prices",
            lambda range: plot.make_prices_section(
                limit_and_resample_df(dataframes["prices"], range).sort_index(axis=1),
                "Prices",
            ).update_layout(margin=subplot_margin),
        ),
        (
            "forex",
            lambda range: plot.make_forex_section(
                limit_and_resample_df(dataframes["forex"], range),
                "Forex",
            ).update_layout(margin=subplot_margin),
        ),
        (
            "interest_rate",
            lambda range: plot.make_interest_rate_section(
                limit_and_resample_df(dataframes["interest_rate"], range)
            ).update_layout(margin=subplot_margin),
        ),
        (
            "brokerage_total",
            lambda range: plot.make_brokerage_total_section(
                limit_and_resample_df(dataframes["brokerages"], range)
            ).update_layout(margin=subplot_margin),
        ),
    ]
    new_graphs: Graphs = {"ranged": defaultdict(dict), "nonranged": {}}
    with parallel_config(n_jobs=-1):
        for name, json in typing.cast(
            tuple,
            Parallel(return_as="generator_unordered")(
                delayed(plot_generate)(*args, layout)
                for args in nonranged_graphs_generate
            ),
        ):
            new_graphs["nonranged"][name] = json
        for r in ranges:
            for name, json in typing.cast(
                tuple,
                Parallel(return_as="generator_unordered")(
                    delayed(plot_generate_ranged)(*args, r, layout)
                    for args in ranged_graphs_generate
                ),
            ):
                new_graphs["ranged"][name][r] = json

    end_time = datetime.now()
    cached_graphs = new_graphs
    last_updated_time = end_time
    last_generation_duration = end_time - start_time
    latest_datapoint_time = dataframes["all"].index[-1]
    logger.info(f"Graph generation time: {last_generation_duration}")
    return GraphData(
        cached_graphs,
        last_updated_time,
        last_generation_duration,
        latest_datapoint_time,
    )


def use_cached_graphs(metadata: Mapping) -> bool:
    """Do not update graphs if history has not changed since last update."""
    latest = common.read_sql_table("history").loc[lambda df: ~df.duplicated()].index[-1]
    return pd.Timestamp.fromtimestamp(metadata["time"]) > latest


@Memory(f"{common.PREFIX}cache", verbose=0).cache(
    cache_validation_callback=use_cached_graphs
)
def clear_and_generate(
    cache_call_args: tuple[tuple[tuple[str, str], ...], list[str], dict[str, int]],
) -> None:
    generate_all_graphs.clear()
    generate_all_graphs(*cache_call_args)
