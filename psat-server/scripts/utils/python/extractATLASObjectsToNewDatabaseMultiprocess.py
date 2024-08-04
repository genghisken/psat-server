#!/usr/bin/env python
# NOTE: This script should NOT be run by root.  It's too dangerous to allow root to run
#       this script. The potential for accidentially deleting the source database is very
#       high.

"""Move ATLAS objects in specified list to another database.  Multiprocessing version.
For safety do NOT use a destination database user that also has write access to the
source database.

Assumes:
  1. The destination user has at least SELECT privileges on the source database.
  2. The databases are in the SAME MySQL server instance.

Usage:
  %s <username> <password> <database> <hostname> <sourceschema> [<candidates>...] [--truncate] [--ddc] [--list=<listid>] [--flagdate=<flagdate>] [--copyimages] [--loglocation=<loglocation>] [--logprefix=<logprefix>] [--dumpfile=<dumpfile>] [--nocreateinfo] [--djangofile=<djangofile>] [--imagessource=<imagessource>] [--imagesdest=<imagesdest>] [--getmetadata] [--insertdiffisubcelllogs] [--includeauthtoken] [--survey=<survey>]
  %s (-h | --help)
  %s --version

Options:
  -h --help                       Show this screen.
  --version                       Show version.
  --list=<listid>                 List to be migrated. Multiple lists should be separated by commas with no spaces [default: 2].
  --flagdate=<flagdate>           Flag date threshold beyond which we will select objects [default: 20170920].
  --ddc                           Assume the DDC schema.
  --truncate                      Truncate the database tables. Default is NOT to truncate.
  --copyimages                    Copy the images as well (extremely time consuming).
  --loglocation=<loglocation>     Log file location [default: /tmp/]
  --logprefix=<logprefix>         Log prefix [default: migration]
  --nocreateinfo                  When dumping db tables from large db, skip the create statements (for inhomogenious backends situation).
  --dumpfile=<dumpfile>           Filename of dumped data [default: /home/atls/big_tables.sql].
  --djangofile=<djangofile>       Filename of dumped Django data [default: /home/atls/django_schema.sql].
  --imagessource=<imagessource>   Source root location of the images [default: /db4/images/].
  --imagesdest=<imagesdest>       Destination location for extracted images [default: /db4/images/].
  --getmetadata                   Get metadata associated with objects, otherwise insert ALL metadata.
  --insertdiffsubcelllogs         Insert diff subcell logs (very large amount of data).
  --includeauthtoken              Include authtoken_token in the Django export.
  --survey=<survey>               Which transient database are we migrating? atlas or panstarrs? [default: atlas]


E.g.:
  %s publicuser publicpass public db1 atlas4 --ddc --list=5 --copyimages
  %s atlas4migrateduser xxxxxxxxxxxxxxx atlas4migrated db1 atlas4 --ddc --list=1,2,3,5,7,8,9,10,11 --copyimages --loglocation=/db4/tc_logs/atlas4/ --includeauthtoken
  %s ps13pi_extracteduser xxxxxxxxxxxxxx ps13pi_extracted db0 ps13pi 1111822080325015100 --copyimages --loglocation=/db0/tc_logs/ps13pi/ --nocreateinfo --dumpfile=/home/pstc/ps13pi/ps13pi_extracted.sql --djangofile=/home/pstc/ps13pi/ps13pi_extracted_django.sql --imagessource=/db0/images/ --imagesdest=/db0/images/ --getmetadata --survey=panstarrs

"""
import sys
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt
import os, MySQLdb, shutil, re
from gkutils.commonutils import dbConnect, find, Struct, cleanOptions
from psat_server_web.atlas.atlas.commonqueries import getNonDetectionsUsingATLASFootprint, LC_POINTS_QUERY_ATLAS_DDC, ATLAS_METADATADDC, filterWhereClauseddc, FILTERS
from extractATLASObjectsToNewDatabase import getATLASObjects, getSpecifiedObjects, migrateData, truncateAllTables, removeAllImagesAndLocationMaps, insertAllRecords, getSpecifiedObjectsPanSTARRS
import gc
from gkutils.commonutils import splitList, parallelProcess
import queue
import datetime


def worker(num, db, listFragment, dateAndTime, firstPass, miscParameters):
    """thread worker function"""
    # Redefine the output to be a log file.
    options = miscParameters[0]
    sys.stdout = open('%s%s_%s_%d.log' % (options.loglocation, options.logprefix, dateAndTime, num), "w")

    conn = dbConnect(options.hostname, options.username, options.password, options.database)
    if not conn:
        print("Cannot connect to the public database")
        return 1

    # 2023-03-07 KWS MySQLdb disables autocommit by default. Switch it on globally.
    conn.autocommit(True)

    connPrivateReadonly = dbConnect(options.hostname, options.username, options.password, options.sourceschema)
    if not connPrivateReadonly:
        print("Cannot connect to the private database")
        return 1

    migrateData(conn, connPrivateReadonly, listFragment, options.database, options.sourceschema, ddc = options.ddc, copyimages = options.copyimages, imageRootSource = options.imagessource, imageRootDestination = options.imagesdest, getmetadata = options.getmetadata)

    print("Process complete.")
    conn.close()
    connPrivateReadonly.close ()
    print("DB Connection Closed - exiting")
    return 0



# ###########################################################################################
#                                         Main program
# ###########################################################################################

def main():
    """main.
    """

    opts = docopt(__doc__, version='0.1')
    opts = cleanOptions(opts)
    options = Struct(**opts)

    detectionList = [2]
    if options.list is not None:
        try:
            detectionList = [int(x) for x in options.list.split(',')]
            if min(detectionList) < 1 or max(detectionList) > 12:
                print("Detection list must be between 1 and 12")
                return 1
        except ValueError as e:
            sys.exit("Detection list must be an integer")

    if options.flagdate is not None:
        try:
            dateThreshold = '%s-%s-%s' % (options.flagdate[0:4], options.flagdate[4:6], options.flagdate[6:8])
        except:
            dateThreshold = '2017-09-16'

    db = []
    db.append(options.username)
    db.append(options.password)
    db.append(options.database)
    db.append(options.hostname)

    candidateList = []

    conn = dbConnect(options.hostname, options.username, options.password, options.database)
    if not conn:
        print("Cannot connect to the new destination database")
        return 1

    # 2023-03-07 KWS MySQLdb disables autocommit by default. Switch it on globally.
    conn.autocommit(True)

    connPrivateReadonly = dbConnect(options.hostname, options.username, options.password, options.sourceschema)
    if not connPrivateReadonly:
        print("Cannot connect to the source (read only) database")
        return 1

    # Supplied candidates override the specified list
    if options.candidates:
        for candidate in options.candidates:
            candidateList.append(int(candidate))
        if options.survey == 'panstarrs':
            candidateList = getSpecifiedObjectsPanSTARRS(connPrivateReadonly, candidateList)
        else:
            candidateList = getSpecifiedObjects(connPrivateReadonly, candidateList)

    else:
        candidateList = []
        for l in detectionList:
            if options.survey == 'panstarrs':
                specifiedList = getPanSTARRSObjects(connPrivateReadonly, listId = l)
            else:
                specifiedList = getATLASObjects(connPrivateReadonly, listId = l)
            if specifiedList:
                candidateList = candidateList + list(specifiedList)

    print("Length of list = %d" % len(candidateList))

    # 2022-11-25 KWS Cannot truncate tables, etc in multiprocessing mode.
    if options.truncate:
        print('Truncating the tables...')
        truncateAllTables(conn, options.database)
        print('Removing the images...')
        removeAllImagesAndLocationMaps(options.database, options.imagesdest)

        # First of all insert the complete tables we definitely want to keep. Must be done single threaded.
        #print('Inserting data into tcs_cmf_metadata...')
        #insertAllRecords(conn, 'tcs_cmf_metadata', options.sourceschema, options.database)
        #print('Inserting data into atlas_metadata...')
        #insertAllRecords(conn, 'atlas_metadata', options.sourceschema, options.database)
        #print('Inserting data into atlas_metadataddc...')
        #insertAllRecords(conn, 'atlas_metadataddc', options.sourceschema, options.database)
        print('Inserting data into tcs_gravity_events...')
        insertAllRecords(conn, 'tcs_gravity_events', options.sourceschema, options.database)
        print('Inserting data into atlas_heatmaps...')
        insertAllRecords(conn, 'atlas_heatmaps', options.sourceschema, options.database)
        print('Inserting data into tcs_object_group_definitions...')
        insertAllRecords(conn, 'tcs_object_group_definitions', options.sourceschema, options.database)
        print('Inserting data into atlas_diff_logs...')
        insertAllRecords(conn, 'atlas_diff_logs', options.sourceschema, options.database)
        print('Inserting data into tcs_processing_status...')
        insertAllRecords(conn, 'tcs_processing_status', options.sourceschema, options.database)
        print('Inserting data into tcs_catalogue_tables...')
        insertAllRecords(conn, 'tcs_catalogue_tables', options.sourceschema, options.database)
        print('Inserting data into tcs_tns_requests...')
        insertAllRecords(conn, 'tcs_tns_requests', options.sourceschema, options.database)
        print('Inserting data into tcs_detection_lists...')
        insertAllRecords(conn, 'tcs_detection_lists', options.sourceschema, options.database)
        print('Inserting data into tcs_gravity_alerts...')
        insertAllRecords(conn, 'tcs_gravity_alerts', options.sourceschema, options.database)

        # 2024-03-14 KWS Added authtoken_token. 
        print('Extracting all the Django relevant tables into a dump file. Requires SELECT and LOCK TABLE access to sourceschema.')
        djangoTables = 'auth_group auth_group_permissions auth_permission auth_user auth_user_groups auth_user_user_permissions django_admin_log django_content_type django_migrations django_session django_site'
        if options.includeauthtoken:
            djangoTables += ' authtoken_token'
        cmd = 'mysqldump -u%s --password=%s %s -h %s %s > %s' % (options.username, options.password, options.sourceschema, options.hostname, djangoTables, options.djangofile)
        os.system(cmd)

        print('Importing the Django tables from the %s schema.' % options.sourceschema)
        cmd = 'mysql -u%s --password=%s %s -h %s < %s' % (options.username, options.password, options.database, options.hostname, options.djangofile)
        os.system(cmd)

        # 2024-03-14 KWS Added tcs_cmf_metadata, atlas_metadata, atlas_metadataddc.
        print('Extracting the very large tables into a dump file. Requires SELECT and LOCK TABLE access to sourceschema.')
        noCreateInfo = ''
        if options.nocreateinfo is not None:
            noCreateInfo = '--no-create-info'

        metatables = ''
        if not options.getmetadata:
            metatables = 'tcs_cmf_metadata atlas_metadata atlas_metadataddc'
        if insertdiffsubcelllogs:
            metatables += ' atlas_diff_subcells atlas_diff_subcell_logs'
        
        if metatables:
            cmd = 'mysqldump -u%s --password=%s %s -h %s %s --no-tablespaces %s > %s' % (options.username, options.password, options.sourceschema, options.hostname, noCreateInfo, metatables, options.dumpfile)
            os.system(cmd)

            print('Importing the giant tables from the %s schema.' % options.sourceschema)
            cmd = 'mysql -u%s --password=%s %s -h %s < %s' % (options.username, options.password, options.database, options.hostname, options.dumpfile)
            os.system(cmd)

    # The following tables contain over 100 million rows each. Doing a standard
    # replace into won't work.  Best to do a dump of the two tables and import
    # just before going live.
#        print('Inserting data into atlas_diff_subcells...')
#        insertAllRecords(conn, 'atlas_diff_subcells', options.sourceschema, options.database)
#        print('Inserting data into atlas_diff_subcell_logs...')
#        insertAllRecords(conn, 'atlas_diff_subcell_logs', options.sourceschema, options.database)

    currentDate = datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")
    (year, month, day, hour, mins, sec) = currentDate.split(':')
    dateAndTime = "%s%s%s_%s%s%s" % (year, month, day, hour, mins, sec)

    if len(candidateList) > 0:
        nProcessors, listChunks = splitList(candidateList)

        print("%s Parallel Processing..." % (datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")))
        parallelProcess(db, dateAndTime, nProcessors, listChunks, worker, miscParameters = [options], drainQueues = False)
        print("%s Done Parallel Processing" % (datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")))


    conn.close ()
    connPrivateReadonly.close ()

    return 0


if __name__ == '__main__':
    main()
