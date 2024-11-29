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
TICKER="IBKR"
sqlite-utils add-column web/sqlite.db schwab_etfs_prices ${TICKER} float
sqlite-utils add-column web/sqlite.db schwab_etfs_amounts ${TICKER} float
sqlite-utils schema web/sqlite.db > sqlite_schema.sql
```

Update `ledger_amounts.py` and `history.py` and `balance_etfs.py` to include new ticker.