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

conda create -n panstarrs python=2.7 pip
conda create -n panstarrs3.6 python=3.6 pip
conda create -n panstarrs39 python=3.9 pip
conda create -n gocart python=3.11 pip
conda create -n dew_old_ml_psdb3 python=2.7 pip
conda create -n sherlock_panstarrs37 python=3.7 pip

# Now install the relevant components into each environment above.

conda activate gocart
sleep 2
pip install -r ${CODEBASE}/scripts/utils/bash/legacy_python_requirements/gocart_20240213.txt --no-cache-dir
sleep 2

conda activate dew_old_ml_psdb3
sleep 2
pip install -r ${CODEBASE}/scripts/utils/bash/legacy_python_requirements/dew_old_ml_psdb3_20240213.txt --no-cache-dir
sleep 2

conda activate panstarrs
sleep 2
pip install -r ${CODEBASE}/scripts/utils/bash/legacy_python_requirements/panstarrs_20240213_no_dill.txt --no-cache-dir
sleep 2

conda activate panstarrs3.6
sleep 2
pip install -r ${CODEBASE}/scripts/utils/bash/legacy_python_requirements/panstarrs3.6_20240213.txt --no-cache-dir
sleep 2

conda activate panstarrs39
sleep 2
pip install -r ${CODEBASE}/scripts/utils/bash/legacy_python_requirements/panstarrs39_20240213.txt --no-cache-dir
sleep 2

conda activate sherlock_panstarrs37
sleep 2
pip install -r ${CODEBASE}/scripts/utils/bash/legacy_python_requirements/sherlock_panstarrs37_20240213.txt --no-cache-dir

