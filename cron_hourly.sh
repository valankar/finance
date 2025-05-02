#!/bin/bash

cd $HOME/code/accounts
docker compose up accounts_hourly --abort-on-container-exit --force-recreate
RETVAL=$?

NEEDS_RUN=0
docker compose -f $HOME/code/accounts/docker-compose.yml exec --workdir /app/code/accounts accounts ./finance_daily.py --methods-run-needed
if [ $? -ne 0 ]; then
  NEEDS_RUN=1
fi
if [ $NEEDS_RUN -eq 1 ]; then
  docker compose up accounts_daily selenium --abort-on-container-exit --force-recreate
  if [ $? -ne 0 ]; then
    # Fail with daily return value
    echo "Daily failure"
    exit 1
  fi
fi

# Use last exit value
exit $RETVAL
