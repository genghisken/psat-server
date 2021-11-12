#!/bin/bash

# I need to include the code root, because the bash script needs the code
# to parse the config file.  I didn't want to have to copy the same piece
# of code every time I wanted to run a bash script.
if [ $# -ne 3 ]
then
    echo "Usage: `basename $0` <code root> <config file> <backup>"
    exit 1
fi

export CODEROOT=$1
export CONFIGFILE=$2

# BACKUP should be one of 'skip', 'xtra' or 'full'
export BACKUP=$3

# We need to specify $CODEROOT, because the code needs to be able to find
# the parse_yaml.sh common code in order to read the yaml file!
. $CODEROOT/scripts/utils/bash/parse_yaml.sh
eval $(parse_yaml $CONFIGFILE)

export DBUSER=$databases__local__username
export DBPASS=$databases__local__password
export DBHOST=$databases__local__hostname
export DBNAME=$databases__local__database
export CODEBASE=$code_paths__codebase
export IPPREPOSITORY=$code_paths__ipprepository
export LOGLOCATION=$code_paths__loglocation
export LOGPREFIX=$code_paths__logprefix
export STAGINGDIRECTORY=$code_paths__stagingdirectory
export DUMP_LOCATION_ROOT=$code_paths__dump_location_root
export OFFSITE_BACKUP_LOCATION=$code_paths__offsite_backup_location
export INGEST_TYPE=$code_paths__ingest_type

export NOW=`date +'%Y-%m-%d_%H:%M:%S'`

echo ""
echo $CODEBASE
echo $IPPREPOSITORY
echo $LOGLOCATION
echo $LOGPREFIX
echo $STAGINGDIRECTORY
echo $DUMP_LOCATION_ROOT
echo $OFFSITE_BACKUP_LOCATION
echo $INGEST_TYPE
echo $BACKUP

export CMF_FILE_LOCATION=$IPPREPOSITORY

NDAYSRSYNC=5

UNIXTIME=$(date '+%s')
# 2018-05-09 KWS We only care about data ingested TODAY!
DAYS=0
NDAYSAGO=$(($UNIXTIME-$DAYS*86400))
echo 'FRUITBAT'
OFFSETDATE=$(date '+%Y%m%d' -d "1970-01-01 $NDAYSAGO sec UTC")
echo 'FRUITBAT'

# 2017-01-30 KWS Added MJD offset
TWENTYONEDAYSAGO=$(($UNIXTIME-21*86400))
MJD21=$(awk -v u=$TWENTYONEDAYSAGO 'BEGIN { print int(40587+u/86400) }')

echo $UNIXTIME
echo $NDAYSAGO
exit

case "$INGEST_TYPE" in
   ATLAS)
      export MULTIPROCESSOR=$CODEBASE/code/ingesters/tphot/python/ingesterWrapperMultiprocess.py
      export MULTIPROCESSOR_ARGUMENTS="$CONFIGFILE"
      export POSTINGESTCUTTER=$CODEBASE/code/database/reports/python/applyAtlasCutsDDCMultiprocess.py
      export POSTINGESTCUTTER_ARGUMENTS="$CONFIGFILE --datethreshold $OFFSETDATE --update --recent"
      ;;

   *)
      echo "$INGEST_TYPE must be 'ATLAS'"
      exit 1
esac

echo ""
echo $MULTIPROCESSOR
echo $MULTIPROCESSOR_ARGUMENTS
echo $POSTINGESTCUTTER $POSTINGESTCUTTER_ARGUMENTS


export LD_LIBRARY_PATH=/opt/rh/python27/root/usr/lib64:/usr/local/lib:/usr/local/swtools/lib

export DB_DUMPER=$CODEBASE/code/database/backup/backup_transient_database_atlas.sh

export PYTHONPATH=$CODEBASE/code/experimental/pstamp/python:$CODEBASE/code/web/atlas/atlas:$CODEBASE/code/lctrend/python:$CODEBASE/code/database/reports/python:$CODEBASE/code/machine_learning:$CODEBASE/code/utils/python:/usr/local/ps1code/gitrelease/dew_machine_learning/PS1-Real-Bogus/tools:/usr/local/ps1code/gitrelease/dew_machine_learning/ufldl

#export PYTHON=/usr/local/swtools/python_virtualenv/users/atls/atlas/bin/python
export PYTHON=/usr/local/swtools/python/atls/anaconda3/envs/atlas/bin/python
#export SHERLOCK=/usr/local/swtools/python/atls/anaconda3/envs/sherlock/bin/sherlock
export SHERLOCK=/usr/local/swtools/python/atls/anaconda3/envs/sherlock37/bin/sherlock
export PYTHON3=/usr/local/swtools/python/atls/anaconda3/envs/atlas3.6/bin/python

echo ""
echo $CMF_FILE_LOCATION
echo $PYTHONPATH
echo ""
# Processing status = 1 = STARTED
# Processing status = 2 = FINISHED

PROCESSINGSTATUS=`mysql -u $DBUSER --password=$DBPASS $DBNAME -h $DBHOST --skip-column-names -Be "select count(*) from tcs_processing_status where status != 2;"`

if [ $PROCESSINGSTATUS -gt 0 ]
then
   echo "Cannot start a new process.  Processing is still continuing."
else
   echo "OK.  Start new process."
   INSERTID=`mysql -u $DBUSER --password=$DBPASS $DBNAME -h $DBHOST --skip-column-names -Be "insert into tcs_processing_status (status, started) values (1, now()); select last_insert_id();"`

   # Dump the database first
   if [ $BACKUP == 'xtra' ] || [ $BACKUP == 'full' ]
   then
     echo `date +%Y%m%d_%H%M%S`: Kick off DB Dump script...
     $DB_DUMPER $CODEBASE $CONFIGFILE $BACKUP > /$DBHOST/$LOGLOCATION/transient_db_dump_`date +%Y%m%d_%H%M`.log 2>&1
   fi

   # Update the database to indicate that DB dumping is now finished.
   mysql -u $DBUSER --password=$DBPASS $DBNAME -h $DBHOST --skip-column-names -Be "update tcs_processing_status set status = 5, modified = now() where id = $INSERTID;"

   # Get the data to ingest and ingest it.

   echo `date +%Y%m%d_%H%M%S`: Downloading MLO data...
   $CODEBASE/code/ingesters/tphot/bash/atlas_data_rsync_recent.sh 01a $NDAYSRSYNC > /$DBHOST/$LOGLOCATION/rsync_`date +%Y%m%d_%H%M`_cron.log 2>&1
   echo `date +%Y%m%d_%H%M%S`: Downloading HKO data...
   $CODEBASE/code/ingesters/tphot/bash/atlas_data_rsync_recent.sh 02a $NDAYSRSYNC > /$DBHOST/$LOGLOCATION/rsync_`date +%Y%m%d_%H%M`_cron.log 2>&1

   # Ingest data from the last n days. (Default is 5 days).
   #echo `date +%Y%m%d_%H%M%S`: Ingesting MLO data...
   #$PYTHON $MULTIPROCESSOR $MULTIPROCESSOR_ARGUMENTS *.ddc --camera 01a --days $NDAYSRSYNC > /$DBHOST/$LOGLOCATION/transient_multiprocessor_`date +%Y%m%d_%H%M`.log 2>&1

   echo `date +%Y%m%d_%H%M%S`: Ingesting HKO data...
   $PYTHON $MULTIPROCESSOR $MULTIPROCESSOR_ARGUMENTS *.ddc --camera 02a --days $NDAYSRSYNC > /$DBHOST/$LOGLOCATION/transient_multiprocessor_`date +%Y%m%d_%H%M`.log 2>&1

   # 2018-11-12 KWS Ingestion is taking so long, so try a second time before
   #                running the cuts.
   echo `date +%Y%m%d_%H%M%S`: Downloading MLO data...
   $CODEBASE/code/ingesters/tphot/bash/atlas_data_rsync_recent.sh 01a $NDAYSRSYNC > /$DBHOST/$LOGLOCATION/rsync_`date +%Y%m%d_%H%M`_cron.log 2>&1
   echo `date +%Y%m%d_%H%M%S`: Downloading HKO data...
   $CODEBASE/code/ingesters/tphot/bash/atlas_data_rsync_recent.sh 02a $NDAYSRSYNC > /$DBHOST/$LOGLOCATION/rsync_`date +%Y%m%d_%H%M`_cron.log 2>&1

   # Ingest data from the last n days. (Default is 5 days).
   #echo `date +%Y%m%d_%H%M%S`: Ingesting MLO data...
   #$PYTHON $MULTIPROCESSOR $MULTIPROCESSOR_ARGUMENTS *.ddc --camera 01a --days $NDAYSRSYNC > /$DBHOST/$LOGLOCATION/transient_multiprocessor_`date +%Y%m%d_%H%M`.log 2>&1
   
   echo `date +%Y%m%d_%H%M%S`: Ingesting HKO data...
   $PYTHON $MULTIPROCESSOR $MULTIPROCESSOR_ARGUMENTS *.ddc --camera 02a --days $NDAYSRSYNC > /$DBHOST/$LOGLOCATION/transient_multiprocessor_`date +%Y%m%d_%H%M`.log 2>&1

   echo `date +%Y%m%d_%H%M%S`: Ingesting Diff Logs...
   $PYTHON $CODEBASE/code/database/reports/python/populateATLASDiffLogTable.py $CONFIGFILE --days 20 --camera=01a > /$DBHOST/$LOGLOCATION/difflog_ingest_`date +%Y%m%d_%H%M`.log 2>&1
   $PYTHON $CODEBASE/code/database/reports/python/populateATLASDiffLogTable.py $CONFIGFILE --days 20 --camera=02a > /$DBHOST/$LOGLOCATION/difflog_ingest_`date +%Y%m%d_%H%M`.log 2>&1

   echo `date +%Y%m%d_%H%M%S`: Counting diff detetion subcell totals...
   $PYTHON $CODEBASE/code/database/reports/python/populateATLASDiffDetectionSubcells.py $CONFIGFILE --days 20 --camera=01a --ddc > /$DBHOST/$LOGLOCATION/subcell_count_`date +%Y%m%d_%H%M`.log 2>&1
   $PYTHON $CODEBASE/code/database/reports/python/populateATLASDiffDetectionSubcells.py $CONFIGFILE --days 20 --camera=02a --ddc > /$DBHOST/$LOGLOCATION/subcell_count_`date +%Y%m%d_%H%M`.log 2>&1

   echo `date +%Y%m%d_%H%M%S`: Running post ingest cuts...
   $PYTHON $POSTINGESTCUTTER $POSTINGESTCUTTER_ARGUMENTS > /$DBHOST/$LOGLOCATION/apply_cuts_`date +%Y%m%d_%H%M`.log 2>&1

   # 2017-11-00 KWS Run the recentre (and merge) code. Make sure that the nearest detection to
   #                the mean RA and Dec is always flagged in the atlas_diff_objects table.
   #                Note that the merge part is not yet complete.
   echo `date +%Y%m%d_%H%M%S`: Recentring the object...
   $PYTHON $CODEBASE/code/database/reports/python/atlasRecentreAndMerge.py $CONFIGFILE --list 4 --update --ddc > /$DBHOST/$LOGLOCATION/atlas_object_recentring_`date +%Y%m%d_%H%M`.log 2>&1

   # Are the re too many objects in the diff subcells?
   echo `date +%Y%m%d_%H%M%S`: Quarantining bad diff subcells
   $PYTHON $CODEBASE/code/database/reports/python/atlasCheckPStar.py $CONFIGFILE --list 4 --update --ddc --diffsubcellthreshold 1000 --diffsubcell > /$DBHOST/$LOGLOCATION/check_subcells_`date +%Y%m%d_%H%M`.log 2>&1

   # Generate daily stats.  We do this twice.  The first time (here) to get the mean RA and Dec.
   # We use the mean RA and Dec in the next stage of context classification and also external
   # crossmatching.
   echo `date +%Y%m%d_%H%M%S`: Generating latest object stats...
   if [ $INGEST_TYPE == 'ATLAS' ]
   then
      $PYTHON $CODEBASE/code/database/reports/python/atlasGenerateDailyStatsMultiprocess.py $CONFIGFILE --ddc > /$DBHOST/$LOGLOCATION/generate_daily_stats_`date +%Y%m%d_%H%M`.log 2>&1
   fi

#   # Are we a bit close to the edge?
#   echo `date +%Y%m%d_%H%M%S`: Quarantining the objects close to the chip edge
#   $PYTHON $CODEBASE/code/database/reports/python/atlasCheckPStar.py $CONFIGFILE --list 4 --coords --update --ddc > /$DBHOST/$LOGLOCATION/check_x_y_median_`date +%Y%m%d_%H%M`.log 2>&1

   # 2015-02-07 KWS Changed classification to be done after eyeball promotion.
   if [ $INGEST_TYPE == 'ATLAS' ]
   then
#      echo `date +%Y%m%d_%H%M%S`: Running post ingest crossmatching algorithm - context classification
#      $PYTHON $CODEBASE/code/database/reports/python/atlasPostIngestCrossmatch.py $CONFIGFILE --update > /$DBHOST/$LOGLOCATION/post_ingest_crossmatch_`date +%Y%m%d_%H%M`.log 2>&1
      # 2017-10-11 KWS Added Sherlock Lite
      echo `date +%Y%m%d_%H%M%S`: Running Sherlock
      $SHERLOCK dbmatch -AN --update -s /home/atls/.config/sherlock/sherlock_atlas4_all.yaml > /$DBHOST/$LOGLOCATION/sherlock_`date +%Y%m%d_%H%M`.log 2>&1
      #ssh psdb3 "/home/atls/anaconda2/bin/sherlock dbmatch -AN --update -s /home/atls/.config/sherlock/sherlock_atlas4_all.yaml" > /$DBHOST/$LOGLOCATION/sherlock_`date +%Y%m%d_%H%M`.log 2>&1
      echo `date +%Y%m%d_%H%M%S`: Move the variable stars VS and Bright Stars BS out to the star list
      mysql -u $DBUSER --password=$DBPASS $DBNAME -h $DBHOST --skip-column-names -Be "update atlas_diff_objects set detection_list_id = 6 where detection_list_id = 4 and (sherlockClassification = 'VS' or sherlockClassification = 'BS');"
   fi

   if [ $INGEST_TYPE == 'ATLAS' ]
   then
      $PYTHON $CODEBASE/code/database/reports/python/atlasQuickMagTrendCheck.py $CONFIGFILE --list 4 --update --ddc > /$DBHOST/$LOGLOCATION/quick_lc_trend_`date +%Y%m%d_%H%M`.log 2>&1
      $PYTHON $CODEBASE/code/database/reports/python/atlasQuickMagTrendCheck.py $CONFIGFILE --list 3 --update --ddc > /$DBHOST/$LOGLOCATION/quick_lc_trend_`date +%Y%m%d_%H%M`.log 2>&1
      $PYTHON $CODEBASE/code/database/reports/python/atlasQuickMagTrendCheck.py $CONFIGFILE --list 2 --update --ddc > /$DBHOST/$LOGLOCATION/quick_lc_trend_`date +%Y%m%d_%H%M`.log 2>&1
      $PYTHON $CODEBASE/code/database/reports/python/atlasQuickMagTrendCheck.py $CONFIGFILE --list 1 --update --ddc > /$DBHOST/$LOGLOCATION/quick_lc_trend_`date +%Y%m%d_%H%M`.log 2>&1
   fi

   # 2015-05-06 KWS Externally crossmatch the eyeball list every day immediately after the list is ready.
   echo `date +%Y%m%d_%H%M%S`: Doing external crossmatch...
   $PYTHON $CODEBASE/code/database/reports/python/atlasPostIngestExternalTransientCheckMultiprocess.py $CONFIGFILE --list=4 --update --updateSNType > /$DBHOST/$LOGLOCATION/atlas_external_transient_check_`date +\%Y\%m\%d_\%H\%M`.log
   $PYTHON $CODEBASE/code/database/reports/python/atlasPostIngestExternalTransientCheckMultiprocess.py $CONFIGFILE --list=2 --update --updateSNType > /$DBHOST/$LOGLOCATION/atlas_external_transient_check_`date +\%Y\%m\%d_\%H\%M`.log
   $PYTHON $CODEBASE/code/database/reports/python/atlasPostIngestExternalTransientCheckMultiprocess.py $CONFIGFILE --list=1 --update --updateSNType > /$DBHOST/$LOGLOCATION/atlas_external_transient_check_`date +\%Y\%m\%d_\%H\%M`.log
   $PYTHON $CODEBASE/code/database/reports/python/atlasPostIngestExternalTransientCheckMultiprocess.py $CONFIGFILE --list=3 --update --updateSNType > /$DBHOST/$LOGLOCATION/atlas_external_transient_check_`date +\%Y\%m\%d_\%H\%M`.log
   $PYTHON $CODEBASE/code/database/reports/python/atlasPostIngestExternalTransientCheckMultiprocess.py $CONFIGFILE --list=5 --update --updateSNType > /$DBHOST/$LOGLOCATION/atlas_external_transient_check_`date +\%Y\%m\%d_\%H\%M`.log

   # Generate daily stats again. This time we want the external crossmatches to appear. 
   echo `date +%Y%m%d_%H%M%S`: Generating latest object stats...
   if [ $INGEST_TYPE == 'ATLAS' ]
   then
      $PYTHON $CODEBASE/code/database/reports/python/atlasGenerateDailyStatsMultiprocess.py $CONFIGFILE --ddc > /$DBHOST/$LOGLOCATION/generate_daily_stats_`date +%Y%m%d_%H%M`.log 2>&1
      $PYTHON $CODEBASE/code/database/reports/python/atlasGenerateDailyStatsMultiprocess.py $CONFIGFILE --list 2 --ddc > /$DBHOST/$LOGLOCATION/generate_daily_stats_`date +%Y%m%d_%H%M`.log 2>&1
      $PYTHON $CODEBASE/code/database/reports/python/atlasGenerateDailyStatsMultiprocess.py $CONFIGFILE --list 3 --ddc > /$DBHOST/$LOGLOCATION/generate_daily_stats_`date +%Y%m%d_%H%M`.log 2>&1
      $PYTHON $CODEBASE/code/database/reports/python/atlasGenerateDailyStatsMultiprocess.py $CONFIGFILE --list 5 --ddc > /$DBHOST/$LOGLOCATION/generate_daily_stats_`date +%Y%m%d_%H%M`.log 2>&1
      $PYTHON $CODEBASE/code/database/reports/python/atlasGenerateDailyStatsMultiprocess.py $CONFIGFILE --list 1 --ddc > /$DBHOST/$LOGLOCATION/generate_daily_stats_`date +%Y%m%d_%H%M`.log 2>&1
   fi

#   # Move the variable stars out of the way
#   echo `date +%Y%m%d_%H%M%S`: Moving variable stars out of the way
#   $PYTHON $CODEBASE/code/database/reports/python/atlasCheckPStar.py $CONFIGFILE --list 4 --update --ddc > /$DBHOST/$LOGLOCATION/check_pvr_median_`date +%Y%m%d_%H%M`.log 2>&1

   # Check for movers
   echo `date +%Y%m%d_%H%M%S`: Checking for movers...
   $PYTHON $CODEBASE/code/database/reports/python/mpcEphemeridesCheckAtlasMultiprocess.py $CONFIGFILE --list 4 --update --ddc > /$DBHOST/$LOGLOCATION/ephemerides_check_`date +%Y%m%d_%H%M`.log 2>&1

   # Check for solar system satellites
   echo `date +%Y%m%d_%H%M%S`: Checking for solar system satellites...
   $PYTHON $CODEBASE/code/database/reports/python/moonMatcherMultiprocess.py $CONFIGFILE --list=4 --update --ddc > /$DBHOST/$LOGLOCATION/moon_check_`date +%Y%m%d_%H%M`.log 2>&1

   # MPC check for any objects already in the good list
   $PYTHON $CODEBASE/code/database/reports/python/atlasMinorPlanetCheck.py $CONFIGFILE --list 2 --update --ddc > /$DBHOST/$LOGLOCATION/atlas_mpc_check_`date +%Y%m%d_%H%M`.log 2>&1

   # Generate finders for objects already in the good list
   $PYTHON $CODEBASE/code/experimental/pstamp/python/getATLASPS1Finders.py $CONFIGFILE --list 2 --ddc > /$DBHOST/$LOGLOCATION/finders_`date +%Y%m%d_%H%M`_cron.log 2>&1

   # Now make the stamps.
   echo `date +%Y%m%d_%H%M%S`: Building the stamps...
   $PYTHON $CODEBASE/code/experimental/pstamp/python/pstampMakeAtlasStamps3Multiprocess.py $CONFIGFILE --list 4 --update --limit 6 --flagdate 20170925 --ddc > /$DBHOST/$LOGLOCATION/pstamp_request_`date +%Y%m%d_%H%M`.log 2>&1

   # Update the objects with the first stamp triplet
   echo `date +%Y%m%d_%H%M%S`: Updating triplets...
   $PYTHON $CODEBASE/code/database/reports/python/atlasPostImageIngestGetMostRecentRow.py $CONFIGFILE  --list 4 --ddc > /$DBHOST/$LOGLOCATION/get_recent_image_row_`date +\%Y\%m\%d_\%H\%M`_cron.log 2>&1
   # Run the machine learning on the stamps.
   echo `date +%Y%m%d_%H%M%S`: Running pixel machine learning...
   $PYTHON $CODEBASE/code/machine_learning/machineClassifyTransientsAtlasMultiprocess.py $CONFIGFILE --list 4 --update > /$DBHOST/$LOGLOCATION/ml_pixel_classifier_`date +\%Y\%m\%d_\%H\%M`_cron.log 2>&1

   # Run the machine learning on the stamps. We need to tunnel onto a machine running CentOS7!
   echo `date +%Y%m%d_%H%M%S`: Running Tensorflow pixel machine learning...
   #cd $CODEBASE/code/database/reports/sql
   #sh getATLASImagesForML.sh $DBNAME 4 > /home/atls/eyeball_list_images.txt
   #ssh kws@s1 "/home/kws/python_centos7/virtualenv/tensorflow/bin/python /home/kws/keras/testImage.py /home/atls/eyeball_list_images.txt" > /$DBHOST/$LOGLOCATION/ml_keras_tf_pixel_classifier_`date +\%Y\%m\%d_\%H\%M`_cron.log 2>&1
   #mysql -u $DBUSER --password=$DBPASS $DBNAME -h $DBHOST --skip-column-names -Be "source /home/kws/keras/update_eyeball_scores.sql;"
#   $PYTHON3 $CODEBASE/../ATLAS-ML/runKerasTensorflowClassifierOnATLASImages.py $CONFIGFILE --hkoclassifier=$CODEBASE/../tf_trained_classifiers/asteroids_20x20_skew3_signpreserve_20191125_classifier.h5 --mloclassifier=$CODEBASE/../tf_trained_classifiers/asteroids_20x20_skew3_signpreserve_20191125_classifier.h5 --outputcsv=/db4/$LOGLOCATION/ml_tf_keras_`date +\%Y\%m\%d_\%H\%M`.csv --listid=4 --update > /$DBHOST/$LOGLOCATION/ml_keras_tf_pixel_classifier_`date +\%Y\%m\%d_\%H\%M`_cron_new.log 2>&1
   $PYTHON3 $CODEBASE/../ATLAS-ML/runKerasTensorflowClassifierOnATLASImages.py $CONFIGFILE --hkoclassifier=$CODEBASE/../tf_trained_classifiers/asteroids122062_good27938_bad450000_20x20_skew3_signpreserve_20200819hko_classifier.h5 --mloclassifier=$CODEBASE/../tf_trained_classifiers/asteroids136521_good13479_bad450000_20x20_skew3_signpreserve_20200819mlo_classifier.h5 --outputcsv=/db4/$LOGLOCATION/ml_tf_keras_`date +\%Y\%m\%d_\%H\%M`.csv --listid=4 --update > /$DBHOST/$LOGLOCATION/ml_keras_tf_pixel_classifier_`date +\%Y\%m\%d_\%H\%M`_cron_new.log 2>&1
   mysql -u $DBUSER --password=$DBPASS $DBNAME -h $DBHOST --skip-column-names -Be "update atlas_diff_objects set detection_list_id = 0 where detection_list_id = 4 and zooniverse_score < 0.2;"

#####   # Ditch any objects that RB factor < 0.1 and have not been flagged by someone else.
######   mysql -u $DBUSER --password=$DBPASS $DBNAME -h $DBHOST --skip-column-names -Be "update atlas_diff_objects o, tcs_latest_object_stats s set detection_list_id = 0 where s.id = o.id and detection_list_id = 4 and realbogus_factor is not null and realbogus_factor < 0.1 and external_crossmatches is null;"
#####

   echo `date +%Y%m%d_%H%M%S`: Moving objects to the Fast Track list
   mysql -u $DBUSER --password=$DBPASS $DBNAME -h $DBHOST --skip-column-names -Be "source $CODEBASE/code/database/reports/sql/move_objects_to_fast_track_list.sql;"
   mysql -u $DBUSER --password=$DBPASS $DBNAME -h $DBHOST --skip-column-names -Be "set @listid:=2; source $CODEBASE/code/database/reports/sql/add_objects_to_faint_and_fast_custom_list.sql;"
   mysql -u $DBUSER --password=$DBPASS $DBNAME -h $DBHOST --skip-column-names -Be "set @listid:=1; source $CODEBASE/code/database/reports/sql/add_objects_to_faint_and_fast_custom_list.sql;"
   mysql -u $DBUSER --password=$DBPASS $DBNAME -h $DBHOST --skip-column-names -Be "set @listid:=2; source $CODEBASE/code/database/reports/sql/add_objects_to_fast_track_custom_list.sql;"
   mysql -u $DBUSER --password=$DBPASS $DBNAME -h $DBHOST --skip-column-names -Be "set @listid:=1; source $CODEBASE/code/database/reports/sql/add_objects_to_fast_track_custom_list.sql;"

   echo `date +%Y%m%d_%H%M%S`: Kicking off Forced photometry for the eyeball list. Do not wait for the results.

   $PYTHON $CODEBASE/code/experimental/pstamp/python/doAtlasForcedPhotometryMultiprocess.py $CONFIGFILE --list 8 --limit 30 --ddc --update --useflagdate --mlscore 0.2 > /$DBHOST/$LOGLOCATION/forced_photometry_`date +%Y%m%d_%H%M`_cron.log 2>&1 &
   $PYTHON $CODEBASE/code/experimental/pstamp/python/doAtlasForcedPhotometryMultiprocess.py $CONFIGFILE --list 4 --limit 30 --ddc --update --useflagdate --mlscore 0.2 > /$DBHOST/$LOGLOCATION/forced_photometry_`date +%Y%m%d_%H%M`_cron.log 2>&1 &

   echo `date +%Y%m%d_%H%M%S`: Done.
   # Update the database to state that the process has completed
   mysql -u $DBUSER --password=$DBPASS $DBNAME -h $DBHOST --skip-column-names -Be "update tcs_processing_status set status = 2, modified = now() where id = $INSERTID;"
fi
