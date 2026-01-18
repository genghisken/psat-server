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
    echo "Usage: `basename $0` <camera> <ndays> <lockfile_location>"
    exit 1
fi

for var in REMOTE_USER REMOTE_SERVER DESTINATION_ROOT; do
    if [[ -z "${!var}" ]]; then
        echo "The $var variable is not set."
        exit 1
    fi
done

export CAMERA=$1
export NDAYS=$2
export LOCKFILE_LOCATION=$3

# 2024-08-21 KWS Introduced a timeout for the rsync command. It will self destruct after TIMEOUT seconds
export TIMEOUT=1200

# We know the following MJDs are bad.
#export SKIPMJDS=57656,57657
export EXCLUDES=""

if [[ ! -z "$SKIPMJDS" ]]
then
  IFS=',' read -r -a mjds <<< "$SKIPMJDS"

  for i in "${mjds[@]}"
  do
     EXCLUDES+=" --exclude=$i*/"
  done
fi

# Calculate today's whole MJD.  Go back NDAYS from today.
# 2023-01-03 KWS Need to add +1 day to today, since S. Africa starts its
#                MJD before midnight.
UNIXTIME=$(date +%s)
MJDTODAY=$(echo $UNIXTIME | awk '{print int($1/86400.0+2440587.5-2400000.5+1)}')
MJDNDAYSAGO=$(($MJDTODAY-$NDAYS-1))

# 2018-01-24 KWS Don't run if there is a lock file.
# 2018-09-11 KWS Use -K option - which forces rsync to follow symlinks on local media.
#                Implemented so that we can rsync from Hawaii to archived data area.
# 2018-09-05 KWS Use minimum encryption for efficiency.
export LOCKFILE=$LOCKFILE_LOCATION/atlas_raw_rsync_lock_recent_$CAMERA
if [ ! -f $LOCKFILE ]
then
  echo "Rsync started: `date +%Y%m%d_%H%M%S`" > $LOCKFILE
  for ((i=MJDNDAYSAGO;i<=MJDTODAY;i++)); do
    # Make the CMD variable an array since timeout will split the command and produce a syntax error
    export CMD=(/usr/bin/rsync -avkKL -e 'ssh -c aes128-ctr -o Compression=no' --exclude=\".*\" "$REMOTE_USER@$REMOTE_SERVER:/atlas/obs/$CAMERA/$i" "$DESTINATION_ROOT/atlas/obs/$CAMERA")

    timeout "$TIMEOUT" "${CMD[@]}"
  done
  rm -f $LOCKFILE
else
  echo "Rsync already running. Remove lockfile to start a new download."
fi
