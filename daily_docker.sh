#!/bin/bash

flock $HOME/code/accounts -c "docker compose -f $HOME/code/accounts/docker-compose.yml run --rm accounts conda run -p $HOME/miniforge3/envs/investing --no-capture-output ./finance_daily.py"