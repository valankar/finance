#!/bin/bash

mamba update --all -n base
mamba update --all -n firefox
mamba update --all -n ledger
mamba update --all -n weight
mamba update --all -n investing
mamba clean -a -y

mamba env export -n firefox >environment-firefox.yml
mamba env export -n ledger >environment-ledger.yml
mamba env export -n weight > ~/code/weight/environment.yml
mamba env export -n investing >environment.yml