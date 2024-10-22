# Finance

This is a project to keep track of finances. It generates graphs like the following:

![Assets Breakdown](examples/assets_breakdown.jpg)
![Investing](examples/investing.jpg)
![Real Estate](examples/realestate.jpg)
![Allocation](examples/allocation.jpg)
![Net Worth](examples/networth.jpg)
![Forex and Funds](examples/forex_funds.jpg)

## How it works

Generally you will not be able to use this as is and it will need lots of modifications. This
is just to get you started.

The `finance_hourly.py` script should be run hourly. The `finance_daily.py` script should be run daily.
This stores all the historical data as text or CSV files, and then creates plots.

Crontab example:

```shell
@hourly             $HOME/code/accounts/cron_hourly.sh
@daily              $HOME/code/accounts/cron_daily.sh
```

Locking the directory is to prevent both scripts writing at the same time.

## Adding new tickers

```shell
sqlite-utils add-column web/sqlite.db schwab_etfs_prices IBKR float
sqlite-utils add-column web/sqlite.db schwab_etfs_amounts IBKR float
sqlite-utils schema web/sqlite.db > sqlite_schema.sql
```

Update `ledger_amounts.py` and `history.py` to include new ticker.