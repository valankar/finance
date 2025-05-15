#!/bin/bash
set -e

HOST=valankar@cachyos-server

# First build locally
cd $HOME/code/accounts
docker compose --profile development down
docker build -t accounts . && docker image prune -f && docker compose --profile development up -d accounts-dev
if [ $? -ne 0 ]; then
  echo "Failed to build locally"
  exit 1
fi

ssh $HOST 'cd code/accounts && docker compose down accounts accounts_hourly accounts_daily'
./rsync.sh || exit 1
ssh $HOST 'cd code/accounts && docker build -t accounts . && docker image prune -f && docker compose up -d'
