#!/bin/bash

# Explicitly set up the pyenv environment
export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init --path)"
eval "$(pyenv init -)"
eval "$(pyenv virtualenv-init -)"

# Change to the project directory and execute the script
cd /home/sn0man/projects/data-universe && /home/sn0man/projects/data-universe/bittensor_venv/bin/python scripts/purge_old_data.py
