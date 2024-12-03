#!/bin/bash

# 2024-03-28 KWS Removed --days since the default is 3.
#export PYTHONPATH=/usr/local/ps1code/gitrelease/stephen/st3ph3n:/usr/local/ps1code/gitrelease/atlasapiclient
#export CONFIG_ATLASAPI=/usr/local/ps1code/gitrelease/atlasapiclient/atlasapiclient/config_files/api_config_MINE.yaml
export CONFIG_ATLASAPI=/home/stevance/software/st3ph3n/st3ph3n/data/api_config_LOCAL.yaml

echo "Starting to update the VRA scores."
t_start=$(date +%s)
python updateVRAScores.py $CONFIG_ATLASAPI --quiet
t_end=$(date +%s)

echo "Finished updating VRA scores."
delta_t=$((t_end - t_start))

echo "Update VRA scores took $delta_t seconds."
