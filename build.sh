#!/bin/bash

HOST=valankar@debian

ssh $HOST 'cd code/accounts && docker compose down accounts accounts_hourly accounts_daily'
./rsync.sh || exit 1
ssh $HOST 'cd code/accounts && docker build -t accounts .'
ssh $HOST 'cd code/accounts && docker compose up -d'