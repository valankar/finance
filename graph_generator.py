from collections import defaultdict
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Callable, Literal

import pandas as pd
from dateutil.relativedelta import relativedelta
from loguru import logger
from plotly.graph_objects import Figure

import common
import plot
import stock_options

type NonRangedGraphs = dict[str, dict]
type RangedGraphs = dict[str, dict[str, dict]]
type Graphs = dict[Literal["ranged", "nonranged"], NonRangedGraphs | RangedGraphs]


def get_xrange(dataframe: pd.DataFrame, selected_range: str) -> tuple[str, str] | None:
    """Determine time range for selected button."""
    today_time = datetime.now()
    today_time_str = today_time.strftime("%Y-%m-%d")
    xrange = None
    relative = None
    match selected_range:
        case "All":
            xrange = (dataframe.index[0].strftime("%Y-%m-%d"), today_time_str)
        case "3y":
            relative = relativedelta(years=-3)
        case "2y":
            relative = relativedelta(years=-2)
        case "1y":
            relative = relativedelta(years=-1)
        case "YTD":
            xrange = (today_time.strftime("%Y-01-01"), today_time_str)
        case "6m":
            relative = relativedelta(months=-6)
        case "3m":
            relative = relativedelta(months=-3)
        case "1m":
            relative = relativedelta(months=-1)
        case "1d":
            relative = relativedelta(days=-1)
    if relative:
        xrange = (
            (today_time + relative).strftime("%Y-%m-%d"),
            today_time_str,
        )
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
        return df.resample(window).mean().interpolate()
    return df


def submit_plot_generator(
    executor: ThreadPoolExecutor,
    name: str,
    plot_func: Callable[[], Figure],
    layout: tuple[tuple[str, str], ...],
) -> Future:
    plot = executor.submit(lambda: plot_func()).result()
    executor.submit(
        lambda: plot.write_image(
            f"{common.PREFIX}/{name}.png",
            width=1024,
            height=768 * get_plot_height_percent(name, layout),
        )
    )
    return executor.submit(lambda: plot.to_plotly_json())


def get_plot_height_percent(name: str, layout: tuple[tuple[str, str], ...]) -> float:
    for n, height in layout:
        if n == name:
            return float(int(height[:-2]) / 100)
    return 1.0


def submit_plot_generator_ranged(
    executor: ThreadPoolExecutor,
    name: str,
    plot_func: Callable[[str], Figure],
    r: str,
    layout: tuple[tuple[str, str], ...],
) -> Future:
    plot = executor.submit(lambda: plot_func(r)).result()
    executor.submit(
        lambda: plot.write_image(
            f"{common.PREFIX}/{name}-{r}.png",
            width=1024,
            height=768 * get_plot_height_percent(name, layout),
        )
    )
    return executor.submit(lambda: plot.to_plotly_json())


@common.cache_decorator
def generate_all_graphs(
    layout: tuple[tuple[str, str], ...],
    ranges: list[str],
    subplot_margin: dict[str, int],
) -> tuple[Graphs, datetime, timedelta, pd.Timestamp]:
    """Generate and save all Plotly graphs."""
    logger.info("Generating graphs")
    start_time = datetime.now()
    dataframes = {
        "all": common.read_sql_table("history").sort_index(),
        "real_estate": common.get_real_estate_df(),
        "prices": (common.read_sql_table("schwab_etfs_prices").sort_index()),
        "forex": common.read_sql_table("forex").sort_index(),
        "interest_rate": plot.get_interest_rate_df(),
        "options": stock_options.options_df(),
    }
    with ThreadPoolExecutor() as executor:
        nonranged_graphs = {
            "allocation_profit": submit_plot_generator(
                executor,
                "allocation_profit",
                lambda: plot.make_allocation_profit_section(
                    dataframes["all"],
                    dataframes["real_estate"],
                ).update_layout(margin=subplot_margin),
                layout,
            ),
            "change": submit_plot_generator(
                executor,
                "change",
                lambda: plot.make_change_section(
                    dataframes["all"],
                    "total",
                    "Total Net Worth Change",
                ),
                layout,
            ),
            "change_no_homes": submit_plot_generator(
                executor,
                "change_no_homes",
                lambda: plot.make_change_section(
                    dataframes["all"],
                    "total_no_homes",
                    "Total Net Worth Change w/o Real Estate",
                ),
                layout,
            ),
            "investing_allocation": submit_plot_generator(
                executor,
                "investing_allocation",
                lambda: plot.make_investing_allocation_section(),
                layout,
            ),
            "loan": submit_plot_generator(
                executor,
                "loan",
                lambda: plot.make_loan_section().update_layout(margin=subplot_margin),
                layout,
            ),
        }
        if len(dataframes["options"]):
            nonranged_graphs["short_options"] = submit_plot_generator(
                executor,
                "short_options",
                lambda: plot.make_short_options_section(
                    dataframes["options"]
                ).update_layout(margin=subplot_margin),
                layout,
            )
        ranged_graphs = defaultdict(dict)
        for r in ranges:
            ranged_graphs["assets_breakdown"][r] = submit_plot_generator_ranged(
                executor,
                "assets_breakdown",
                lambda range: plot.make_assets_breakdown_section(
                    limit_and_resample_df(dataframes["all"], range)
                ).update_layout(margin=subplot_margin),
                r,
                layout,
            )
            ranged_graphs["investing_retirement"][r] = submit_plot_generator_ranged(
                executor,
                "investing_retirement",
                lambda range: plot.make_investing_retirement_section(
                    limit_and_resample_df(
                        dataframes["all"][["pillar2", "ira", "commodities", "etfs"]],
                        range,
                    )
                ).update_layout(margin=subplot_margin),
                r,
                layout,
            )
            ranged_graphs["real_estate"][r] = submit_plot_generator_ranged(
                executor,
                "real_estate",
                lambda range: plot.make_real_estate_section(
                    limit_and_resample_df(
                        dataframes["real_estate"],
                        range,
                    )
                ).update_layout(margin=subplot_margin),
                r,
                layout,
            )
            ranged_graphs["prices"][r] = submit_plot_generator_ranged(
                executor,
                "prices",
                lambda range: plot.make_prices_section(
                    limit_and_resample_df(dataframes["prices"], range).sort_index(
                        axis=1
                    ),
                    "Prices",
                ).update_layout(margin=subplot_margin),
                r,
                layout,
            )
            ranged_graphs["forex"][r] = submit_plot_generator_ranged(
                executor,
                "forex",
                lambda range: plot.make_forex_section(
                    limit_and_resample_df(dataframes["forex"], range),
                    "Forex",
                ).update_layout(margin=subplot_margin),
                r,
                layout,
            )
            ranged_graphs["interest_rate"][r] = submit_plot_generator_ranged(
                executor,
                "interest_rate",
                lambda range: plot.make_interest_rate_section(
                    limit_and_resample_df(dataframes["interest_rate"], range)
                ).update_layout(margin=subplot_margin),
                r,
                layout,
            )

        new_graphs: Graphs = {"ranged": defaultdict(dict), "nonranged": {}}
        for name, future in nonranged_graphs.items():
            new_graphs["nonranged"][name] = future.result()
        for name, ranged in ranged_graphs.items():
            for r, future in ranged.items():
                new_graphs["ranged"][name][r] = future.result()
    end_time = datetime.now()
    cached_graphs = new_graphs
    last_updated_time = end_time
    last_generation_duration = end_time - start_time
    latest_datapoint_time = dataframes["all"].index[-1]
    logger.info(f"Graph generation time: {last_generation_duration}")
    return (
        cached_graphs,
        last_updated_time,
        last_generation_duration,
        latest_datapoint_time,
    )
