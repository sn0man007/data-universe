#!/bin/bash

# Set up the pyenv environment to ensure the correct Python version and packages are used
export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init --path)"
eval "$(pyenv init -)"
eval "$(pyenv virtualenv-init -)"

# Change to the project directory and execute the pipeline script
cd /home/sn0man/projects/data-universe && /home/sn0man/projects/data-universe/bittensor_venv/bin/python scripts/run_pipeline_cycle.py
