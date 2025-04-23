#!/bin/bash

cd $HOME/code/accounts
docker compose up accounts_hourly --abort-on-container-exit --force-recreate
RETVAL=$?

# If there was a daily failure or it needs a run, re-run it.
NEEDS_RUN=0
docker logs accounts_daily 2>&1 | grep ERROR
if [ $? -eq 0 ]; then
  NEEDS_RUN=1
fi
docker compose -f $HOME/code/accounts/docker-compose.yml exec --workdir /app/code/accounts accounts ./finance_daily.py --methods-run-needed
if [ $? -eq 1 ]; then
  NEEDS_RUN=1
fi
if [ $NEEDS_RUN -eq 1 ]; then
  docker compose up accounts_daily selenium --abort-on-container-exit --force-recreate
fi

exit $RETVAL

