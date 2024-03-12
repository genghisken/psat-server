#!/bin/bash

# Rename a database from one schema to a new EMPTY schema. This does NOT (and cannot) move the views.
# Recreate the views on the new database AFTER the move. The user must have full permissions on both schemas.

if [ $# -ne 1 ]
then
   echo "Usage: `basename $0` <ddcfile>"
   exit 1
fi

# Check that ddc.nnc file does NOT exist. Abort if already exists.
# Check that there is BOTH a ddc file AND an nnc file (otherwise abort)
# If possible, skip duplicate columns in nnc file, but the duplicate columns will be stored in the nnc table.

pid=$$
ddcFile=$1
expnameFilePrefix=${ddcFile%.ddc}
nncFile=${expnameFilePrefix}.nnc
colmerge=/atlas/bin/cm

# 2016-03-22 KWS Complete rewrite required.  We CANNOT use unix sorting unless both values of x
#                and y in both files are identical.  If one has more decimal places, the sorting
#                breaks down.  Use JT colmerge code instead and grep -v the duplicates.

# 2017-02-10 KWS John has moved the moments files to AUX from 57793 onwards!
ddcBasename=$(basename "$ddcFile")
ddcDirname=$(dirname "$ddcFile")
mjd=${ddcBasename:3:5}

# Grab the headers. First the main ddc header.
grep "^#" $ddcFile | sed '$d' > ${expnameFilePrefix}.ddc.nnc

# Now grab the column headers in each file and paste them together
ddccolumns=`grep "^#" $ddcFile | tail -1`
nnccolumns=`head -1 $nncFile | tr '#' ' '`
echo "$ddccolumns $nnccolumns" >> ${expnameFilePrefix}.ddc.nnc
$colmerge 5,6,10 $ddcFile 5,6,10 $nncFile | grep -v ' D ' | tr '|' ' ' >> ${expnameFilePrefix}.ddc.nnc
