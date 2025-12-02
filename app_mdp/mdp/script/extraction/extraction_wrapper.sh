#!/bin/bash

scheduler_id="$1"
pos_dt="$2"
config_file_path="$3"
param4="$4"
param5="$5"

# Initialize variables
run_only_task=""
overwrite_config=""

# Determine if param4 and/or param5 should be assigned to run_only_task or overwrite_config
# Check if param4 or param5 looks like JSON (for overwrite_config)
task_regex='^[a-zA-Z0-9_-]+(,[a-zA-Z0-9_-]+)*$'

if [[ -n "$param4" ]]; then
    if [[ "$param4" =~ $task_regex ]]; then
        run_only_task="$param4"
    else
        overwrite_config="$param4"
    fi
fi

if [[ -n "$param5" ]]; then
    if [[ "$param5" =~ $task_regex ]]; then
        run_only_task="$param5"
    else
        overwrite_config="$param5"
    fi
fi

# Start building the command
command="mdp_extraction_framework --scheduler_id=$scheduler_id --pos_dt=$pos_dt --config_file_path=$config_file_path"

# Append the overwrite_config if present
if [[ -n $overwrite_config ]]; then
    command="$command --overwrite_config=$overwrite_config"
fi

# Append the run_only_task if present
if [[ -n $run_only_task ]]; then
    command="$command --run_only_task=$run_only_task"
fi

# Execute the command
eval $command
