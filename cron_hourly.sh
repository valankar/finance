#!/bin/bash

cd $HOME/code/accounts
docker compose up accounts_hourly --abort-on-container-exit --force-recreate
