#!/bin/bash

flock $HOME/code/accounts -c "cd $HOME/code/accounts; docker compose up accounts_hourly; docker compose down accounts_hourly" &> $HOME/code/accounts/web/hourly.log
