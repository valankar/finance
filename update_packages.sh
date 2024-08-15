#!/bin/bash

mamba update --all -y -n base
mamba update --all -y -n investing
mamba clean -a -y

mamba env export -n investing --no-builds >environment.yml