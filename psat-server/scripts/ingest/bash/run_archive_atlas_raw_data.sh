#!/bin/bash
if [ $# -ne 6 ]
then
    echo "Usage: `basename $0` <kelvin userid> <archive server> <backup script> <cameras_csv> <ndays> <lockfile location>"
    echo "E.g.: `basename $0` 1234567 kelvin ./archive_atlas_raw_data.sh 01a,02a,03a,04a,05r 10 /tmp"
    exit 1
fi

for var in REMOTE_USER REMOTE_SERVER DESTINATION_ROOT; do
    if [[ -z "${!var}" ]]; then
        echo "The $var variable is not set."
        exit 1
    fi
done

export ARCHIVEUSERID=$1
export ARCHIVESERVER=$2
export BACKUPSCRIPT=$3

export CAMERAS=$4
export NDAYS=$5
export LOCKFILE_LOCATION=$6

ssh "$ARCHIVEUSERID@$ARCHIVESERVER" \
    "REMOTE_USER='$REMOTE_USER' \
     REMOTE_SERVER='$REMOTE_SERVER' \
     DESTINATION_ROOT='$DESTINATION_ROOT' \
     bash '$BACKUPSCRIPT' '$CAMERAS' '$NDAYS' '$LOCKFILE_LOCATION'"


#export ARCHIVECOMMAND="ssh $ARCHIVEUSERID@$ARCHIVESERVER \"\"REMOTE_USER=$REMOTE_USER REMOTE_SERVER=$REMOTE_SERVER DESTINATION_ROOT=$DESTINATION_ROOT bash -c $BACKUPSCRIPT $CANERAS $NDAYS $LOCKFILE_LOCATION\"\""

#$ARCHIVECOMMAND
