#!/bin/bash

# This code checks the last WEEK of MJDs.  The reason for this code is to stop
# rsync from falling over every time someone in Hawaii updates historic data.
# We only really need the most recent data for ingest!

if [ $# -ne 2 ]
then
    echo "Usage: `basename $0` <camera> <ndays>"
    exit 1
fi

export CAMERA=$1
export NDAYS=$2

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
# 2024-01-12 KWS Include the new .nnc files, which will eventually replace .ddc files.
FILE=/tmp/rsync_lock_recent_red_meta_$CAMERA
if [ ! -f $FILE ]
then
  echo "Rsync started: `date +%Y%m%d_%H%M%S`" > $FILE
  for ((i=MJDNDAYSAGO;i<=MJDTODAY;i++)); do
    export CMD="/usr/bin/rsync -avxKL --ignore-existing $EXCLUDES --exclude="OLD/" --include="*/" --include "*.meta" --exclude="*" ksmith@atlas-base-sc01.ifa.hawaii.edu:/atlas/red/$CAMERA/$i /atlas/red/$CAMERA"
    timeout $TIMEOUT $CMD
  done
  rm -f $FILE
else
  echo "Rsync already running. Remove lockfile to start a new download."
fi
