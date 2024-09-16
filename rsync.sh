#!/bin/bash

HOST=valankar@debian
rsync -av --delete --exclude={'web/','__pycache__','.venv/'} $HOME/code/accounts/ $HOST:code/accounts/
rsync -av --delete --exclude={'*.ledger','prices.db','__pycache__'} $HOME/code/ledger/ $HOST:code/ledger/

rsync -av --delete $HOST:code/accounts/web/ $HOME/code/accounts/web/
rsync -av $HOST:code/ledger/{ledger.ledger,prices.db} $HOME/code/ledger/

if [ ! -z "$1" ]; then
    echo "Restarting via docker"
    ssh $HOST 'cd code/accounts && docker compose restart'
fi
