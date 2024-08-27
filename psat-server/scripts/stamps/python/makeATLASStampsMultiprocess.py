#!/usr/bin/env python
"""Make ATLAS Stamps in the context of the transient server database.

Usage:
  %s <configfile> [<candidate>...] [--detectionlist=<detectionlist>] [--customlist=<customlist>] [--limit=<limit>] [--earliest] [--nondetections] [--discoverylimit=<discoverylimit>] [--lastdetectionlimit=<lastdetectionlimit>] [--requesttype=<requesttype>] [--wpwarp=<wpwarp>] [--update] [--ddc] [--skipdownload] [--loglocation=<loglocation>] [--logprefix=<logprefix>] [--loglocationdownloads=<loglocationdownloads>] [--logprefixdownloads=<logprefixdownloads>] [--redregex=<redregex>] [--diffregex=<diffregex>] [--redlocation=<redlocation>] [--difflocation=<difflocation>] [--flagdate=<flagdate>] [--downloadthreads=<downloadthreads>]
  %s (-h | --help)
  %s --version

Options:
  -h --help                                       Show this screen.
  --version                                       Show version.
  --update                                        Update the database
  --detectionlist=<detectionlist>                 List option
  --customlist=<customlist>                       Custom List option
  --limit=<limit>                                 Number of detections for which to request images [default: 6]
  --earliest                                      By default, get the most recent stamps. Otherwise get the earliest ones.
  --nondetections                                 Request non-detections.
  --discoverylimit=<discoverylimit>               Number of days before which we will not request images (ignored if non-detections not requested) [default: 10]
  --lastdetectionlimit=<lastdetectionlimit>       Number of days after the last detection we will request images (ignored if non-detections not requested) [default: 20]
  --requesttype=<requesttype>                     Request type (all | incremental) [default: incremental]
  --ddc                                           Use the DDC schema for queries
  --skipdownload                                  Do not attempt to download the exposures (assumes they already exist locally)
  --wpwarp=<wpwarp>                               Which version of wpwarp to use? (1 or 2) [default: 2]
  --loglocation=<loglocation>                     Log file location [default: /tmp/]
  --logprefix=<logprefix>                         Log prefix [default: stamp_cutter]
  --loglocationdownloads=<loglocationdownloads>   Downloader log file location [default: /tmp/]
  --logprefixdownloads=<logprefixdownloads>       Downloader log prefix [default: stamp_downloader]
  --redregex=<redregex>                           Reduced image regular expression. Caps = variable. [default: EXPNAME.fits.fz]
  --diffregex=<diffregex>                         Diff image regular expression. Caps = variable. [default: EXPNAME.diff.fz]
  --redlocation=<redlocation>                     Reduced image location. E.g. /atlas/diff/CAMERA/fake/MJD.fake (caps = special variable).  Null value means use standard ATLAS archive location.
  --difflocation=<difflocation>                   Diff image location. E.g. /atlas/diff/CAMERA/fake/MJD.fake (caps = special variable). Null value means use standard ATLAS archive location.
  --flagdate=<flagdate>                           Date threshold - no hyphens [default: 20151220].
  --downloadthreads=<downloadthreads>             Number of threads to use for image downloads [default: 10].

E.g.:
  %s ~/config_fakers.yaml --detectionlist=4 --ddc --skipdownload --redlocation=/atlas/diff/CAMERA/fake/MJD.fake --redregex=EXPNAME.fits+fake --difflocation=/atlas/diff/CAMERA/fake/MJD.fake --diffregex=EXPNAME.diff+fake
  %s ../../../../../atlas/config/config4_db5.yaml 1180256580662542100 --ddc --limit=8 --requesttype=all --nondetections --loglocation=/db5/tc_logs/atlas4/ --logprefix=stamp_cutter_1180256580662542100 --loglocationdownloads=/db5/tc_logs/atlas4/ --logprefixdownloads=stamp_downloader_1180256580662542100

"""

import sys
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt
import os, shutil, re, csv, subprocess




import sys, os
import datetime
import subprocess
from gkutils.commonutils import dbConnect, PROCESSING_FLAGS, Struct, cleanOptions
import MySQLdb
from makeATLASStamps import getUniqueExposures, downloadExposures, makeATLASObjectPostageStamps3, getObjectsByList, getObjectsByCustomList
from pstamp_utils import REQUESTTYPES

import gc
from gkutils.commonutils import splitList, parallelProcess
import queue

def workerImageDownloader(num, db, listFragment, dateAndTime, firstPass, miscParameters):
    """thread worker function"""
    # Redefine the output to be a log file.
    options = miscParameters[0]
    sys.stdout = open('%s%s_%s_%d.log' % (options.loglocationdownloads, options.logprefixdownloads, dateAndTime, num), "w")

    # Call the postage stamp downloader
    objectsForUpdate = downloadExposures(listFragment)
    #q.put(objectsForUpdate)
    print("Process complete.")
    return 0

def workerStampCutter(num, db, listFragment, dateAndTime, firstPass, miscParameters):
    """thread worker function"""
    # Redefine the output to be a log file.
    options = miscParameters[8]
    sys.stdout = open('%s%s_%s_%d.log' % (options.loglocation, options.logprefix, dateAndTime, num), "w")
    conn = dbConnect(db[3], db[0], db[1], db[2])
    # 2023-03-13 KWS MySQLdb disables autocommit by default. Switch it on globally.
    conn.autocommit(True)


    PSSImageRootLocation = '/' + db[3] + '/images/' + db[2]
    limit = miscParameters[0]
    mostRecent = miscParameters[1]
    nondetections = miscParameters[2]
    discoverylimit = miscParameters[3]
    lastdetectionlimit = miscParameters[4]
    requestType = miscParameters[5]
    ddc = miscParameters[6]
    wpwarp = miscParameters[7]

    # Call the postage stamp downloader
    objectsForUpdate = makeATLASObjectPostageStamps3(conn, listFragment, PSSImageRootLocation, limit = limit, mostRecent = mostRecent, nonDets = nondetections, discoveryLimit = discoverylimit, lastDetectionLimit=lastdetectionlimit, requestType = requestType, ddc = ddc, wpwarp = wpwarp, options = options)
    #q.put(objectsForUpdate)

    print("Process complete.")
    conn.close()
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

    configFile = options.configfile

    import yaml
    with open(configFile) as yaml_file:
        config = yaml.load(yaml_file)

    username = config['databases']['local']['username']
    password = config['databases']['local']['password']
    database = config['databases']['local']['database']
    hostname = config['databases']['local']['hostname']

    MAX_NUMBER_OF_OBJECTS = int(config['postage_stamp_parameters']['max_number_of_objects'])

    db = []
    db.append(username)
    db.append(password)
    db.append(database)
    db.append(hostname)


    detectionList = 1
    customList = None

    conn = dbConnect(hostname, username, password, database)
    # 2023-03-13 KWS MySQLdb disables autocommit by default. Switch it on globally.
    conn.autocommit(True)


    update = options.update
    limit = int(options.limit)
    mostRecent = not(options.earliest)
    nondetections = options.nondetections
    discoverylimit = int(options.discoverylimit)
    lastdetectionlimit = int(options.lastdetectionlimit)

    objectList = []

    try:
        requestType = REQUESTTYPES[options.requesttype]
    except KeyError as e:
        requestType = REQUESTTYPES['incremental']

    print("REQUEST TYPE = ", requestType)

    flagDate = '2015-12-20'
    if options.flagdate is not None:
        try:
            flagDate = '%s-%s-%s' % (options.flagdate[0:4], options.flagdate[4:6], options.flagdate[6:8])
        except:
            flagDate = '2015-12-20'

    if options.candidate is not None and len(options.candidate) > 0:
        for cand in options.candidate:
            objectList.append({'id': int(cand)})
    else:

        if options.customlist is not None:
            if int(options.customlist) > 0 and int(options.customlist) < 100:
                customList = int(options.customlist)
                objectList = getObjectsByCustomList(conn, customList)
            else:
                print("The list must be between 1 and 100 inclusive.  Exiting.")
                sys.exit(1)
        else:
            if options.detectionlist is not None:
                if int(options.detectionlist) >= 0 and int(options.detectionlist) < 9:
                    detectionList = int(options.detectionlist)
                    objectList = getObjectsByList(conn, listId = detectionList, dateThreshold = flagDate)
                else:
                    print("The list must be between 0 and 6 inclusive.  Exiting.")
                    sys.exit(1)



    currentDate = datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")
    (year, month, day, hour, min, sec) = currentDate.split(':')
    dateAndTime = "%s%s%s_%s%s%s" % (year, month, day, hour, min, sec)

    if len(objectList) > MAX_NUMBER_OF_OBJECTS:
        sys.stderr.write("The number of objects (%d) exceeds the maximum allowed (%d). Cannot continue.\n" % (len(objectList), MAX_NUMBER_OF_OBJECTS))
        sys.exit(1)

    # Only download exposures if requested. Otherwise assume we already HAVE the data.
    if not options.skipdownload:
        exposureSet = getUniqueExposures(conn, objectList, limit = limit, mostRecent = mostRecent, nonDets = nondetections, discoveryLimit = discoverylimit, lastDetectionLimit=lastdetectionlimit, requestType = requestType, ddc = options.ddc)

        # Download threads with multiprocessing - try 10 threads by default
        print("TOTAL OBJECTS = %d" % len(exposureSet))

        print("Downloading exposures...")

        if len(exposureSet) > 0:
            nProcessors, listChunks = splitList(exposureSet, bins = int(options.downloadthreads))

            print("%s Parallel Processing..." % (datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")))
            parallelProcess(db, dateAndTime, nProcessors, listChunks, workerImageDownloader, miscParameters = [options], drainQueues = False)
            print("%s Done Parallel Processing" % (datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")))

            # Belt and braces. Do again, with one less thread.
            nProcessors, listChunks = splitList(exposureSet, bins = int(options.downloadthreads) - 1)

            print("%s Parallel Processing..." % (datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")))
            parallelProcess(db, dateAndTime, nProcessors, listChunks, workerImageDownloader, miscParameters = [options], drainQueues = False)
            print("%s Done Parallel Processing" % (datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")))

    # Produce stamps with multiprocessing - try n(CPUs) threads by default
    print("Producing stamps...")

    if len(objectList) > 0:
        nProcessors, listChunks = splitList(objectList, bins = 48)

        print("%s Parallel Processing..." % (datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")))
        parallelProcess(db, dateAndTime, nProcessors, listChunks, workerStampCutter, miscParameters = [limit, mostRecent, nondetections, discoverylimit, lastdetectionlimit, requestType, options.ddc, options.wpwarp, options], drainQueues = False)
        print("%s Done Parallel Processing" % (datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")))

    conn.close()

    return 0



if __name__ == '__main__':
    main()
