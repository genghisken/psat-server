#!/bin/bash
#  VRA garbage collector wrapper script.

if [ $# -ne 2 ]
then
    echo "Usage: `basename $0` <code root> <config file>"
    exit 1
fi

export CODEROOT=$1
export CONFIGFILE=$2

# We need to specify $CODEROOT, because the code needs to be able to find
# the parse_yaml.sh common code in order to read the yaml file!
if [[ ! -f "$CODEROOT/scripts/utils/bash/parse_yaml.sh" ]]
then
    echo "Cannot find the YAML parser ($CODEROOT/scripts/utils/bash/parse_yaml.sh)"
    exit
fi

. $CODEROOT/scripts/utils/bash/parse_yaml.sh
eval $(parse_yaml $CONFIGFILE)

export DBUSER=$databases__local__username
export DBPASS=$databases__local__password
export DBHOST=$databases__local__hostname
export DBNAME=$databases__local__database
export CODEBASE=$code_paths__codebase
export LOGLOCATION=$code_paths__loglocation
export LOGPREFIX=$code_paths__logprefix

# Belt & braces! If one of the variables above is not set, don't bother continuing.
if [[ ! $DBNAME ]]
then
    echo Cannot find the config file. Aborting.
    exit
fi

# equivalent to conda activate vra
export PYTHONVRA=/usr/local/swtools/python/atls/anaconda3/envs/vra/bin/python
export PYTHONPATH=/usr/local/ps1code/gitrelease/stephen/st3ph3n:/usr/local/ps1code/gitrelease/atlasapiclient
export CONFIG_ATLASAPI=/usr/local/ps1code/gitrelease/atlasapiclient/atlasapiclient/config_files/api_config_MINE.yaml

export OUTPUT_FILE=/$DBHOST/$LOGLOCATION/garbage_`date +%Y%m%d_%H%M`_vra.log

$PYTHONVRA /usr/local/ps1code/gitrelease/psat-server/psat-server/scripts/utils/python/VRAGarbageCollector.py $CONFIG_ATLASAPI $OUTPUT_FILE

#export OUTPUT_FILE=/$DBHOST/$LOGLOCATION/garbage_20240821_1517_vra.log

export ids=`cat $OUTPUT_FILE | tr '\n' ',' | sed -e 's/,$//'`
export sqldelete="update atlas_diff_objects set detection_list_id=0, local_comments='VRA Garbage Collection', date_modified=now() where id in ($ids);"

echo "Moving retired objects to the garbage list."
mysql -u$DBUSER --password="$DBPASS" $DBNAME -h$DBHOST -Be "$sqldelete"
echo "Done."

