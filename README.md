# Finance

This is a project to keep track of finances. It generates graphs like the following:

![Screenshot](examples/screenshot.jpeg)

## How it works

Generally you will not be able to use this as is and it will need lots of modifications. This
is just to get you started.

The `cron_hourly.sh` script should be run hourly. This stores all the historical data and creates plots.

Crontab example:

```shell
@hourly             $HOME/code/accounts/cron_hourly.sh
```

## Adding new indices

```shell
# For indices.
TICKER="^SSMI"
./add_ticker.py --ticker ^SPX
```

Update `balance_etfs.py` to include new ticker.

## Debugging playright

```shell
docker compose up -d selenium-dev
SELENIUM_REMOTE_URL="http://localhost:4444" ./homes.py
```

