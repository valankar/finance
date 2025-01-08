# Finance

This is a project to keep track of finances. It generates graphs like the following:

![Screenshot](examples/screenshot.jpeg)

## How it works

Generally you will not be able to use this as is and it will need lots of modifications. This
is just to get you started.

The `finance_hourly.py` script should be run hourly. The `finance_daily.py` script should be run daily.
This stores all the historical data and creates plots.

Crontab example:

```shell
@hourly             $HOME/code/accounts/cron_hourly.sh
@daily              $HOME/code/accounts/cron_daily.sh
```

## Adding new tickers

```shell
# For ETFs or stocks.
TICKER="IBKR"
sqlite-utils add-column web/sqlite.db schwab_etfs_prices ${TICKER} float
sqlite-utils add-column web/sqlite.db schwab_etfs_amounts ${TICKER} float

# For indices.
TICKER="^SSMI"
sqlite-utils add-column web/sqlite.db index_prices ${TICKER} float

# Update schema.
sqlite-utils schema web/sqlite.db > sqlite_schema.sql
```

Update `common.py` (`COMMODITIES_REGEX`) and `balance_etfs.py` to include new ticker.

## Debugging playright

```shell
docker compose up -d selenium-dev
SELENIUM_REMOTE_URL="http://localhost:4444" ./homes.py
```