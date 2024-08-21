#!/bin/bash

# Initialise the VRA scores wrapper script.

if [ $# -ne 2 ]
then
    echo "Usage: `basename $0` <RB_SCORES_CSV> <RBTHRESHOLD>"
    exit 1
fi

export RB_SCORES_CSV=$1
export RBTHRESHOLD=$2

# equivalent to conda activate vra
export PYTHONVRA=/usr/local/swtools/python/atls/anaconda3/envs/vra/bin/python
export PYTHONPATH=/usr/local/ps1code/gitrelease/stephen/st3ph3n:/usr/local/ps1code/gitrelease/atlasapiclient
#export RB_SCORES_CSV=/db5/tc_logs/atlas4/ml_tf_keras_20240718_0305.csv
export CONFIG_ATLASAPI=/usr/local/ps1code/gitrelease/atlasapiclient/atlasapiclient/config_files/api_config_MINE.yaml

$PYTHONVRA /usr/local/ps1code/gitrelease/psat-server/psat-server/scripts/utils/python/initialiseVRAScores.py $CONFIG_ATLASAPI $RB_SCORES_CSV --rbthreshold=$RBTHRESHOLD