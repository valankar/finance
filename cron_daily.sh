#!/bin/bash

cd $HOME/code/accounts
docker compose up accounts_daily selenium --abort-on-container-exit --force-recreate
