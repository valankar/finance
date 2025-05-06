#!/bin/bash
set -e

HOST=valankar@cachyos-server

# First build locally
cd $HOME/code/accounts
docker compose --profile development down
docker build -t accounts .
docker compose --profile development up -d accounts-dev
docker image prune -f

ssh $HOST 'cd code/accounts && docker compose down accounts accounts_hourly accounts_daily'
./rsync.sh || exit 1
ssh $HOST 'cd code/accounts && docker build -t accounts .'
ssh $HOST 'cd code/accounts && docker compose up -d'
