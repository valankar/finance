#!/bin/bash

cd $HOME/code/accounts
docker compose up accounts_hourly --abort-on-container-exit --force-recreate
RETVAL=$?

docker compose exec --workdir /app/code/accounts accounts ./finance_daily.py --methods-run-needed
if [ $? -ne 0 ]; then
  docker compose up accounts_daily selenium --abort-on-container-exit --force-recreate --no-attach selenium
  if [ $? -ne 0 ]; then
    echo "Daily failure"
    exit 1
  fi
fi

# Use hourly exit value
exit $RETVAL
