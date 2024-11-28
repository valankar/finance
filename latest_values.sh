#!/bin/bash

SQLITE_CMD="sqlite3 ${HOME}/code/accounts/web/sqlite.db -readonly"
SQL_PREFIX="select datetime(date, 'localtime') as date, \
    format('%,.0f', total) as total, \
    format('%,.0f', total_real_estate) as total_real_estate, \
    format('%,.0f', total_no_homes) as total_no_homes, \
    format('%,.0f', total_retirement) as total_retirement, \
    format('%,.0f', total_investing) as total_investing, \
    format('%,.0f', etfs) as etfs, \
    format('%,.0f', commodities) as commodities, \
    format('%,.0f', total_liquid) as total_liquid \
    from history"

# get_ticker failures
$SQLITE_CMD -header -column \
    "select datetime(date, 'localtime') as date, name, error from (\
    select * from function_result where success=False and date > datetime('now', '-1 day') order by date desc limit 5 \
    ) order by date asc;"
echo

# Latest values
$SQLITE_CMD -header -column \
    "$SQL_PREFIX where date < datetime('now', '-1 day') order by date desc limit 1;"
$SQLITE_CMD -column \
    "$SQL_PREFIX order by date desc limit 1;"

