#!/bin/bash

rsync -av --delete --exclude={'web/','__pycache__'} $HOME/code/accounts/ debian:code/accounts/
rsync -av --delete --exclude={'ledger.ledger','__pycache__'} --exclude 'prices.db' $HOME/code/ledger/ debian:code/ledger/

rsync -av --delete debian:code/accounts/web/ $HOME/code/accounts/web/
rsync -av debian:code/ledger/{ledger.ledger,prices.db} $HOME/code/ledger/
