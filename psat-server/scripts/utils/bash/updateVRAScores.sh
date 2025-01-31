#!/bin/bash

# Initialise the VRA scores wrapper script.

if [ $# -ne 1 ]
then
    echo "Usage: `basename $0` <RANKCOLUMN>"
    exit 1
fi

export RANKCOLUMN=$1

# 2024-03-28 KWS Removed --days since the default is 3.
export PYTHONPATH=/usr/local/ps1code/gitrelease/atlasapiclient:/usr/local/ps1code/gitrelease/atlasvras
#export PYTHONPATH=/usr/local/ps1code/gitrelease/stephen/st3ph3n:/usr/local/ps1code/gitrelease/atlasapiclient
export CONFIG_ATLASAPI=/usr/local/ps1code/gitrelease/atlasapiclient/atlasapiclient/config_files/api_config_MINE.yaml


echo "Starting to update the VRA scores."
t_start=$(date +%s)
/usr/local/swtools/python/atls/anaconda3/envs/vra/bin/python /usr/local/ps1code/gitrelease/psat-server/psat-server/scripts/utils/python/updateVRAScores.py $CONFIG_ATLASAPI --rankcolumn=$RANKCOLUMN --quiet
t_end=$(date +%s)

echo "Finished updating VRA scores."
delta_t=$((t_end - t_start))

echo "Update VRA scores took $delta_t seconds."
