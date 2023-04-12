#!/usr/bin/env python3

import math
from datetime import datetime

import numpy as np
import pandas as pd
import plotly.express as px
import pytz
from dateutil.relativedelta import relativedelta

TIMEZONE = 'Europe/Zurich'
TODAY_TIME = datetime.now().astimezone(pytz.timezone(TIMEZONE))


def build_ranges(dataframe, columns):
    """Build graph ranges based on min/max values."""
    today = TODAY_TIME.strftime('%Y-%m-%d')
    xranges = {
        'All': [dataframe.index[0].strftime('%Y-%m-%d'), today],
        '2y': [(TODAY_TIME + relativedelta(years=-2)).strftime('%Y-%m-%d'),
               today],
        '1y': [(TODAY_TIME + relativedelta(years=-1)).strftime('%Y-%m-%d'),
               today],
        'YTD': [TODAY_TIME.strftime('%Y-01-01'), today],
        '6m': [(TODAY_TIME + relativedelta(months=-6)).strftime('%Y-%m-%d'),
               today],
        '3m': [(TODAY_TIME + relativedelta(months=-3)).strftime('%Y-%m-%d'),
               today],
        '1m': [(TODAY_TIME + relativedelta(months=-1)).strftime('%Y-%m-%d'),
               today],
    }
    ranges = {}
    for column in columns:
        col_dict = {}
        for span, xrange in xranges.items():
            col_dict[span] = {
                'yrange': [
                    dataframe.loc[xrange[0]:xrange[1], column].min(),
                    dataframe.loc[xrange[0]:xrange[1], column].max(),
                ],
                'xrange':
                xrange,
            }
        ranges[column] = col_dict
    return ranges


def add_range_buttons(subplot, dataframe, columns):
    """Add a range selector that updates y axis as well as x."""
    ranges = build_ranges(dataframe, columns)
    num_col = len(columns)
    col_split = list(
        reversed(np.array_split(np.array(columns), math.ceil(num_col / 2))))
    buttons = []
    for label in ('All', '2y', '1y', 'YTD', '6m', '3m', '1m'):
        button_dict = dict(
            label=label,
            method='relayout',
        )
        arg_dict = {}
        col = 0
        for pair in col_split:
            for col_name in pair:
                suffix = ''
                if col == 0:
                    arg_dict['xaxis.range'] = ranges[col_name][label]['xrange']
                else:
                    suffix = f'{col + 1}'
                arg_dict[f'yaxis{suffix}.range'] = ranges[col_name][label][
                    'yrange']
                col += 1
            if len(pair) < 2:
                col += 1
        button_dict['args'] = [arg_dict]
        buttons.append(button_dict)
    subplot.update_layout(updatemenus=[
        dict(
            type='buttons',
            direction='right',
            active=2,  # 1y
            x=0.5,
            y=-0.05,
            buttons=buttons,
        )
    ])
    # Select button 2.
    subplot.plotly_relayout(buttons[2]['args'][0])


def make_subplots(daily_df):
    """Create subplots with range selector."""
    columns = ['var1', 'var2', 'var3', 'var4', 'var5', 'var6']
    section = px.line(daily_df,
                      x=daily_df.index,
                      y=columns,
                      facet_col='variable',
                      facet_col_wrap=2,
                      category_orders={'variable': columns})
    section.update_yaxes(matches=None, title_text='')
    section.update_yaxes(col=2, showticklabels=True)
    section.update_xaxes(title_text='', matches='x', showticklabels=True)
    section.update_traces(showlegend=False)
    add_range_buttons(section, daily_df, columns)
    return section


def downsample_df(dataframe):
    """Downsample data older than 1 week."""
    weekly = dataframe.resample('W').mean()
    daily = dataframe.resample('D').mean()
    weekly_concat = weekly[:daily.iloc[-7].name]
    daily_concat = daily[-7:]
    if weekly_concat.iloc[-1].name == daily.iloc[-7].name:
        daily_concat = daily[-6:]
    return pd.concat([weekly_concat, daily_concat])


def main():
    """Main."""
    all_df = pd.read_csv(
        'web/history_example.csv',
        index_col=0,
        parse_dates=True,
        infer_datetime_format=True).tz_localize('UTC').tz_convert(TIMEZONE)

    daily_df = all_df.resample('D').mean().interpolate()

    subplot = make_subplots(downsample_df(daily_df))
    with open('web/foo.html', 'w', encoding='utf-8') as index_file:
        index_file.write(
            subplot.to_html(full_html=True, include_plotlyjs='cdn'))


if __name__ == '__main__':
    main()
