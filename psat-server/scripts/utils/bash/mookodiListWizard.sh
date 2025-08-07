#!/bin/bash

# Initialise the VRA scores wrapper script.

export PYTHONPATH=/usr/local/ps1code/gitrelease/atlasapiclient
export CONFIG_ATLASAPI=/usr/local/ps1code/gitrelease/atlasapiclient/atlasapiclient/config_files/api_config_MINE.yaml

echo "Cleanup and populate Mookodi lists."
t_start=$(date +%s)
/usr/local/swtools/python/atls/anaconda3/envs/vra/bin/python /usr/local/ps1code/gitrelease/psat-server/psat-server/scripts/utils/python/mookodiListWizard.py
t_end=$(date +%s)

echo "Finished with Mookodi lists."
delta_t=$((t_end - t_start))

echo "Mookodi update took $delta_t seconds."
