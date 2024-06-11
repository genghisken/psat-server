#!/bin/bash

# 2024-03-28 KWS Removed --days since the default is 3.
export PYTHONPATH=/usr/local/ps1code/gitrelease/st3ph3n

echo "Starting to update the VRA scores."
t_start=$(date +%s)
/usr/local/swtools/python/atls/anaconda3/envs/st3ph3n/bin/python /usr/local/ps1code/gitrelease/psat-server/psat-server/scripts/utils/python/updateVRAScores.py --quiet
t_end=$(date +%s)

echo "Finished updating VRA scores."
delta_t=$((t_end - t_start))

echo "Update VRA scores took $delta_t seconds."
