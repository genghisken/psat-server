#!/usr/bin/env python
"""Do ATLAS forced photometry.

Usage:
  %s <configfile> [<candidate>...] [--detectionlist=<detectionlist>] [--customlist=<customlist>] [--limit=<limit>] [--limitafter=<limitafter>] [--update] [--ddc] [--skipdownload] [--loglocation=<loglocation>] [--logprefix=<logprefix>] [--loglocationdownloads=<loglocationdownloads>] [--logprefixdownloads=<logprefixdownloads>] [--redregex=<redregex>] [--diffregex=<diffregex>] [--redlocation=<redlocation>] [--difflocation=<difflocation>] [--tphorce=<tphorcelocation>] [--flagdate=<flagdate>] [--downloadthreads=<downloadthreads>] [--mlscore=<mlscore>] [--useflagdate] [--test]
  %s (-h | --help)
  %s --version

Options:
  -h --help                                       Show this screen.
  --version                                       Show version.
  --update                                        Update the database
  --detectionlist=<detectionlist>                 List option
  --customlist=<customlist>                       Custom List option
  --limit=<limit>                                 Number of days before first detection (or flag date) to check [default: 0]
  --limitafter=<limitafter>                       Number of days after first detection (or flag date) to check [default: 0]
  --ddc                                           Use the DDC schema for queries
  --skipdownload                                  Do not attempt to download the exposures (assumes they already exist locally)
  --loglocation=<loglocation>                     Log file location [default: /tmp/]
  --logprefix=<logprefix>                         Log prefix [default: forced_photometry]
  --loglocationdownloads=<loglocationdownloads>   Downloader log file location [default: /tmp/]
  --logprefixdownloads=<logprefixdownloads>       Downloader log prefix [default: forced_downloader]
  --redregex=<redregex>                           Reduced image regular expression. Caps = variable. [default: EXPNAME.fits.fz]
  --diffregex=<diffregex>                         Diff image regular expression. Caps = variable. [default: EXPNAME.diff.fz]
  --redlocation=<redlocation>                     Reduced image location. E.g. /atlas/diff/CAMERA/fake/MJD.fake (caps = special variable).  Null value means use standard ATLAS archive location.
  --difflocation=<difflocation>                   Diff image location. E.g. /atlas/diff/CAMERA/fake/MJD.fake (caps = special variable). Null value means use standard ATLAS archive location.
  --tphorce=<tphorcelocation>                     Location of the tphorce shell script [default: /usr/local/ps1code/gitrelease/tphorce/tphorce].
  --flagdate=<flagdate>                           Date threshold - no hyphens [default: 20151220].
  --downloadthreads=<downloadthreads>             Number of threads to use for image downloads [default: 10].
  --mlscore=<mlscore>                             ML score below which we will NOT request forced photometry.
  --useflagdate                                   Use the flag date as the threshold for the number of days instead of the first detection (which might be rogue).
  --test                                          Just list the exposures for which we will do forced photometry.

E.g.:
  %s ../../../../../atlas/config/config4_db1.yaml --detectionlist=2 --limit=30 --limitafter=150 --ddc --update --useflagdate --loglocation=/tmp/ --loglocationdownloads=/tmp/ --flagdate=20220101
  %s ../../../../../atlas/config/config4_db1.yaml 1192751580350243700 --limit=30 --limitafter=150 --ddc --update --useflagdate --loglocation=/tmp/ --loglocationdownloads=/tmp/

"""

import sys, os
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt
import os, shutil, re, csv, subprocess

import datetime
import subprocess
from gkutils.commonutils import dbConnect, PROCESSING_FLAGS, Struct, cleanOptions
import MySQLdb
from pstamp_utils import REQUESTTYPES

import gc
from gkutils.commonutils import splitList, parallelProcess
import queue

from makeATLASStamps import getObjectsByList, getObjectsByCustomList
from getATLASForcedPhotometry import getForcedPhotometryUniqueExposures, doForcedPhotometry, downloadFPExposures, insertForcedPhotometry, getATLASObject


def workerExposureDownloader(num, db, listFragment, dateAndTime, firstPass, miscParameters):
    """thread worker function"""
    # Redefine the output to be a log file.
    options = miscParameters[0]
    sys.stdout = open('%s%s_%s_%d.log' % (options.loglocationdownloads, options.logprefixdownloads, dateAndTime, num), "w")

    # Call the postage stamp downloader
    objectsForUpdate = downloadFPExposures(listFragment)
    #q.put(objectsForUpdate)
    print("Process complete.")
    return 0

def workerForcedPhotometry(num, db, listFragment, dateAndTime, firstPass, miscParameters, q):
    """thread worker function"""
    # Redefine the output to be a log file.
    options = miscParameters[0]
    sys.stdout = open('%s%s_%s_%d.log' % (options.loglocation, options.logprefix, dateAndTime, num), "w")

    perObjectExps = miscParameters[1]

    # Call the postage stamp downloader
    objectsForUpdate = doForcedPhotometry(options, listFragment, perObjectExps)

    # Write the objects for update onto a Queue object
    print("Adding %d objects onto the queue." % len(objectsForUpdate))

    q.put(objectsForUpdate)

    print("Process complete.")
    return 0


# ###########################################################################################
#                                         Main program
# ###########################################################################################

def main(argv = None):
    """main.

    Args:
        argv:
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

    update = options.update
    limit = int(options.limit)
    limitafter = int(options.limitafter)

    mlscore = None
    if options.mlscore is not None:
        mlscore = float(options.mlscore)


    objectList = []


    flagDate = '2015-12-20'
    if options.flagdate is not None:
        try:
            flagDate = '%s-%s-%s' % (options.flagdate[0:4], options.flagdate[4:6], options.flagdate[6:8])
        except:
            flagDate = '2015-12-20'

    if options.candidate is not None and len(options.candidate) > 0:
        for cand in options.candidate:
            obj = getATLASObject(conn, objectId = int(cand))
            if obj:
                objectList.append(obj)
    else:

        if options.customlist is not None:
            if int(options.customlist) > 0 and int(options.customlist) < 100:
                customList = int(options.customlist)
                objectList = getObjectsByCustomList(conn, customList, processingFlags = 0)
            else:
                print("The list must be between 1 and 100 inclusive.  Exiting.")
                sys.exit(1)
        else:
            if options.detectionlist is not None:
                if int(options.detectionlist) >= 0 and int(options.detectionlist) < 9:
                    detectionList = int(options.detectionlist)
                    objectList = getObjectsByList(conn, listId = detectionList, dateThreshold = flagDate, processingFlags = 0)
                else:
                    print("The list must be between 0 and 9 inclusive.  Exiting.")
                    sys.exit(1)

    print("LENGTH OF OBJECTLIST = ", len(objectList))

    if mlscore is not None and not (options.candidate): # Only do this filter if the IDs are not provided explicitly.
        updatedList = []
        for row in objectList:
            if row['zooniverse_score'] is not None and row['zooniverse_score'] >= mlscore:
                updatedList.append(row)
        if len(updatedList) > 0:
            objectList = updatedList
            print("LENGTH OF CLIPPED OBJECTLIST = ", len(objectList))

    currentDate = datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")
    (year, month, day, hour, min, sec) = currentDate.split(':')
    dateAndTime = "%s%s%s_%s%s%s" % (year, month, day, hour, min, sec)

    # Single threaded
    #perObjectExps, exposureSet = getForcedPhotometryUniqueExposures(conn, objectList, discoveryLimit = limit, ddc = options.ddc, useFlagDate = options.useflagdate)
    perObjectExps, exposureSet = getForcedPhotometryUniqueExposures(conn, objectList, discoveryLimit = limit, cutoffLimit = limitafter, ddc = options.ddc, useFlagDate = options.useflagdate)
    if options.test:
        for obj in objectList:
            print(obj['id'])
            for exp in perObjectExps[obj['id']]['exps']:
                print(exp)
        return 0
    # We'll hand the entire perObjectExps dictionary to each thread.


    # Download threads with multiprocessing - try 10 threads by default
    print("TOTAL OBJECTS = %d" % len(exposureSet))

    print("Downloading exposures...")

    if not options.skipdownload:
        if len(exposureSet) > 0:
            nProcessors, listChunks = splitList(exposureSet, bins = int(options.downloadthreads))

            print("%s Parallel Processing..." % (datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")))
            parallelProcess(db, dateAndTime, nProcessors, listChunks, workerExposureDownloader, miscParameters = [options], drainQueues = False)
            print("%s Done Parallel Processing" % (datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")))

            # Belt and braces - try again with one less thread, just in case the previous one failed.
            nProcessors, listChunks = splitList(exposureSet, bins = int(options.downloadthreads) - 1)

            print("%s Parallel Processing..." % (datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")))
            parallelProcess(db, dateAndTime, nProcessors, listChunks, workerExposureDownloader, miscParameters = [options], drainQueues = False)
            print("%s Done Parallel Processing" % (datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")))

    # Produce stamps with multiprocessing - try n(CPUs) threads by default
    print("Doing Forced Photometry...")

    if len(objectList) > 0:
        nProcessors, listChunks = splitList(objectList)

        print("%s Parallel Processing..." % (datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")))
        objectsForUpdate = parallelProcess(db, dateAndTime, nProcessors, listChunks, workerForcedPhotometry, miscParameters = [options, perObjectExps])
        print("%s Done Parallel Processing" % (datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")))
        if len(objectsForUpdate) > 0 and update:
            insertForcedPhotometry(conn, objectsForUpdate)

    conn.close()

    return 0



if __name__ == '__main__':
    main()
    
