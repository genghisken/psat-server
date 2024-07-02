#!/bin/bash


if [ $# -ne 2 ]
then
   echo "Usage: `basename $0` <codebase> <anaconda_home>"
   echo "E.g.: `basename $0` /usr/local/ps1code/gitrelease/psat-server/psat-server /usr/local/swtools/python/pstc/anaconda3"
   exit 1
fi

export CODEBASE=$1
export ANACONDA_HOME=$2

# anaconda-init
. "${ANACONDA_HOME}/etc/profile.d/conda.sh"

conda create -n atlas python=2.7 pip
conda create -n atlas3.6 python=3.6 pip
conda create -n atlas37 python=3.7 pip
conda create -n sherlock37 python=3.7 pip
conda create -n gocart python=3.11 pip
conda create -n ligo python=3.11 pip
conda create -n st3ph3n python=3.11 pip
conda create -n fastfinder python=3.11 pip

# Now install the relevant components into each environment above.


conda activate atlas3.6
sleep 2
pip install -r ${CODEBASE}/scripts/utils/bash/legacy_python_requirements/requirements_atlas3.6_20240325.txt --no-cache-dir
sleep 2

conda activate atlas37
sleep 2
pip install -r ${CODEBASE}/scripts/utils/bash/legacy_python_requirements/requirements_atlas37_20240325.txt --no-cache-dir
sleep 2

conda activate atlas
sleep 2
pip install -r ${CODEBASE}/scripts/utils/bash/legacy_python_requirements/requirements_atlas_20240325.txt --no-cache-dir
sleep 2

conda activate fastfinder
sleep 2
pip install -r ${CODEBASE}/scripts/utils/bash/legacy_python_requirements/requirements_fastfinder_20240325.txt --no-cache-dir
sleep 2

conda activate gocart
sleep 2
pip install -r ${CODEBASE}/scripts/utils/bash/legacy_python_requirements/requirements_gocart_atlas_20240325.txt --no-cache-dir
sleep 2

conda activate ligo
sleep 2
pip install -r ${CODEBASE}/scripts/utils/bash/legacy_python_requirements/requirements_ligo_20240325.txt --no-cache-dir
sleep 2

conda activate sherlock37
sleep 2
pip install -r ${CODEBASE}/scripts/utils/bash/legacy_python_requirements/requirements_sherlock37_20240325.txt --no-cache-dir
sleep 2

conda activate st3ph3n
sleep 2
pip install -r ${CODEBASE}/scripts/utils/bash/legacy_python_requirements/requirements_st3ph3n_20240325.txt --no-cache-dir
sleep 2

