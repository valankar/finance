#!/bin/bash

[[ $(pgrep gunicorn) ]] || exec $HOME/miniforge3/condabin/mamba run -n investing --cwd $HOME/code/accounts --live-stream gunicorn dashboard:server -b 0.0.0.0:8050 -w 2
