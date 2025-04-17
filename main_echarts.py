import typing

import pandas as pd
from nicegui import ui
from pyecharts import options as opts
from pyecharts.charts import Line

import common


class ECharts:
    def __init__(self):
        self.history_df = common.read_sql_table("history")

    def create_line_chart(self, df: pd.DataFrame, col: str, title: str):
        idx = typing.cast(pd.DatetimeIndex, df.index)
        values = df[col]
        graph = (
            Line()
            .add_xaxis(list(idx.strftime("%Y-%m-%d %H:%M:%S")))
            .add_yaxis(
                series_name=title,
                is_symbol_show=False,
                y_axis=values.tolist(),
                markline_opts=opts.MarkLineOpts(
                    data=[opts.MarkLineItem(y=round(values.iloc[-1]))]
                ),
            )
            .set_global_opts(
                xaxis_opts=opts.AxisOpts(
                    type_="time", splitline_opts=opts.SplitLineOpts(is_show=False)
                ),
                yaxis_opts=opts.AxisOpts(
                    min_=values.min(),
                    max_=values.max(),
                    splitline_opts=opts.SplitLineOpts(is_show=False),
                ),
                title_opts=opts.TitleOpts(title=title),
            )
        )
        ui.echart.from_pyecharts(graph).classes("h-full")

    def create(self):
        with ui.grid(rows=3, columns=2).classes("w-full h-screen gap-0"):
            self.create_line_chart(self.history_df, "total", "Total")
            self.create_line_chart(self.history_df, "total_real_estate", "Real Estate")
            self.create_line_chart(
                self.history_df, "total_no_homes", "Total w/o Real Estate"
            )
            self.create_line_chart(self.history_df, "total_retirement", "Retirement")
            self.create_line_chart(self.history_df, "total_investing", "Investing")
            self.create_line_chart(self.history_df, "total_liquid", "Liquid")
