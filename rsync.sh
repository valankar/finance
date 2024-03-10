#!/bin/bash

HOST=valankar@debian
rsync -av --delete --exclude={'web/','__pycache__'} $HOME/code/accounts/ $HOST:code/accounts/
rsync -av --delete --exclude={'ledger.ledger','__pycache__'} --exclude 'prices.db' $HOME/code/ledger/ $HOST:code/ledger/

rsync -av --delete $HOST:code/accounts/web/ $HOME/code/accounts/web/
rsync -av $HOST:code/ledger/{ledger.ledger,prices.db} $HOME/code/ledger/
