#!/bin/bash

cd $HOME/code/accounts

textual serve --host '*' --port 8081 --url 'https://ledger.valankar.org' ledger_add.py &

./app.py &

wait -n
# Exit with status of process that exited first
exit $?
