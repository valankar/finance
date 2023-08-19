#!/bin/bash

[[ $(pgrep gunicorn) ]] || exec $HOME/miniforge3/condabin/conda run -n investing --cwd $HOME/code/accounts gunicorn dashboard:server -b 0.0.0.0:8050 -w 2 &> /dev/null
