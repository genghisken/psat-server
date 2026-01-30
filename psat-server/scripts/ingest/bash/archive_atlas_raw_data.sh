#!/bin/bash

# Script to copy the ATLAS RAW data from Hawaii to the backup area at QUB.
# We will now operate a window of transfers rather than trying to copy the
# entire directory tree. This is faster, but also allows us to archive
# data onto tape without re-copying the data again from Hawaii.

# The following variables MUST be set:
# REMOTE_USER (e.g. xfer)
# REMOTE_SERVER (e.g. atlas-base-adm02.ifa.hawaii.edu)
# DESTINATION_ROOT (e.g. /mnt/autofs/mcclayrds-nobackup/ad00018)

if [ $# -ne 3 ]
then
    echo "Usage: `basename $0` <cameras_csv> <ndays> <lockfile location>"
    exit
fi

for var in REMOTE_USER REMOTE_SERVER DESTINATION_ROOT; do
    if [[ -z "${!var}" ]]; then
        echo "The $var variable is not set."
        exit 1
    fi
done

export CAMERAS=$1
export NDAYS=$2
export LOCKFILE_LOCATION=$3

IFS=',' read -r -a cameras <<< "$CAMERAS"

for c in "${cameras[@]}"
do
  ./rsync_raw_atlas_data_recent.sh $c $NDAYS $LOCKFILE_LOCATION > raw_atlas_data_${c}_`date +'%Y%m%d_%H%M%S'`.log 2>&1 &
done

