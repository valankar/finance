#!/bin/bash

rsync -av --delete --exclude 'web/' $HOME/code/accounts/ debian:code/accounts/
rsync -av --delete --exclude 'ledger.ledger' --exclude 'prices.db' $HOME/code/ledger/ debian:code/ledger/