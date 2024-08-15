#!/bin/bash

cd $HOME/code/accounts
SCRIPT="$1" docker compose up accounts_daily_script selenium --abort-on-container-exit --force-recreate --attach-dependencies
