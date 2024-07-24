#!/bin/bash

flock $HOME/code/accounts -c "cd $HOME/code/accounts; docker compose up accounts_daily; docker compose down accounts_daily selenium" &> $HOME/code/accounts/web/daily.log
