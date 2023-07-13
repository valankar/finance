#!/bin/bash

export TZ=Europe/Zurich

SQLITE_CMD="sqlite3 web/sqlite.db -readonly"
SQL_PREFIX="select datetime(date, 'localtime') as date, \
    format('%,.0f', total) as total, \
    format('%,.0f', total_real_estate) as total_real_estate, \
    format('%,.0f', total_no_homes) as total_no_homes, \
    format('%,.0f', total_retirement) as total_retirement, \
    format('%,.0f', total_investing) as total_investing, \
    format('%,.0f', etfs) as etfs, \
    format('%,.0f', commodities) as commodities, \
    format('%,.0f', total_liquid) as total_liquid \
    from history order by rowid desc limit 1"

$SQLITE_CMD -header -column \
    "$SQL_PREFIX offset 24;"

$SQLITE_CMD -column \
    "$SQL_PREFIX;"
