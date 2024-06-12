#!/bin/bash

# Join the ddc and nnc files together for ingest into the database.

if [ $# -ne 1 ]
then
    echo "Usage: `basename $0` <ddcfile>"
    exit 1
fi

pid=$$
ddcFile=$1
expnameFilePrefix=${ddcFile%.ddc}
nncFile=${expnameFilePrefix}.nnc

# Check that there is BOTH a ddc file AND an nnc file (otherwise abort)
if ! [ -e "$ddcFile" ] || ! [ -e "$nncFile" ]
then
    echo "BOTH ddc and nnc files must exist. Aborting."
    exit 1
fi

# Check that dnc file does NOT exist. Abort if already exists.
if [[ -e ${expnameFilePrefix}.dnc ]]
then
    echo "Already have a dnc file. Aborting."
    exit 1
fi

# If possible, skip duplicate columns in nnc file, but the duplicate columns will be stored in the nnc table.

colmerge=$(which cm)

if [[ -z $colmerge ]]
then
    echo "Cannot find cm. Aborting."
    exit 1
fi

ddcBasename=$(basename "$ddcFile")
ddcDirname=$(dirname "$ddcFile")
mjd=${ddcBasename:3:5}

# Grab the headers. First the main ddc header.
grep "^#" $ddcFile | sed '$d' > ${expnameFilePrefix}.dnc

# Now grab the column headers in each file and paste them together
ddccolumns=`grep "^#" $ddcFile | tail -1`
nnccolumns=`head -1 $nncFile | sed -e 's/ train.*//' | tr '#' ' ' | sed -e 's/ \{1,\}/ nnc_/g'`
echo "$ddccolumns $nnccolumns" >> ${expnameFilePrefix}.dnc
$colmerge 5,6,10 $ddcFile 5,6,10 $nncFile | grep -v ' D ' | tr '|' ' ' >> ${expnameFilePrefix}.dnc
