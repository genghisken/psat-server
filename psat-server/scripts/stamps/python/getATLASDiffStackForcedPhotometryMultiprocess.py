#!/usr/bin/env python
"""Generate and load up the stacked forced photometry for this object.

Usage:
  %s <configfile> [<candidate>...] [--detectionlist=<detectionlist>] [--customlist=<customlist>] [--update] [--days=<days>] [--mjdmin=<mjdmin>] [--mjdmax=<mjdmax>] [--loglocation=<loglocation>] [--logprefix=<logprefix>] [--flagdate=<flagdate>] [--numberOfThreads=<n>]
  %s (-h | --help)
  %s --version

Options:
  -h --help                         Show this screen.
  --version                         Show version.
  --update                          Update the database.
  --detectionlist=<detectionlist>   Object detection list [default: 4].
  --customlist=<customlist>         Object custom list.
  --days=<days>                     Max number of days to go back before flag date or now [default: 30].
  --mjdmin=<mjdmin>                 Min MJD (overrides Max number of days above).
  --mjdmax=<mjdmax>                 Max MJD (overrides Max number of days above).
  --loglocation=<loglocation>       Log file location [default: /tmp/].
  --logprefix=<logprefix>           Log prefix [default: diffstack_forced_photometry].
  --flagdate=<flagdate>             Date threshold - no hyphens [default: 20240101].
  --numberOfThreads=<n>             Number of threads (prevents remote ATLAS data store overload) [default: 10].

  Example:
    %s ../../../../../atlas/config/config4_db5_readonly.yaml 1161021541115506200 --update
"""
import sys
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt
import os, MySQLdb, shutil, re
from gkutils.commonutils import find, Struct, cleanOptions, dbConnect, readGenericDataFile, calculateRMSScatter, getCurrentMJD, getMJDFromSqlDate
from makeATLASStamps import getObjectsByList, getObjectsByCustomList
from getATLASForcedPhotometry import getATLASObject
import MySQLdb
import datetime
import subprocess

import gc
from gkutils.commonutils import splitList, parallelProcess
import queue

from makeATLASStamps import getObjectsByList, getObjectsByCustomList
from getATLASDiffStackForcedPhotometry import insertStackDetRow, doStackedForcedPhotometry


def workerDiffStackForcedPhotometry(num, db, listFragment, dateAndTime, firstPass, miscParameters, q):
    """thread worker function"""
    # Redefine the output to be a log file.
    options = miscParameters[0]
    sys.stdout = open('%s%s_%s_%d.log' % (options.loglocation, options.logprefix, dateAndTime, num), "w")

    conn = dbConnect(db[3], db[0], db[1], db[2])
    conn.autocommit(True)


    # Call the postage stamp downloader
    objectsForUpdate = doStackedForcedPhotometry(conn, options, listFragment)

    # Write the objects for update onto a Queue object
    print("Adding %d objects onto the queue." % len(objectsForUpdate))

    q.put(objectsForUpdate)
    print("Process complete.")
    conn.close()
    print("DB Connection Closed - exiting")
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
    # 2023-03-13 KWS MySQLdb disables autocommit by default. Switch it on globally.
    conn.autocommit(True)


    update = options.update

    objectList = []


    flagDate = '2024-01-01'
    if options.flagdate is not None:
        try:
            flagDate = '%s-%s-%s' % (options.flagdate[0:4], options.flagdate[4:6], options.flagdate[6:8])
        except:
            flagDate = '2024-01-01'

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
                if int(options.detectionlist) >= 0 and int(options.detectionlist) < 11:
                    detectionList = int(options.detectionlist)
                    objectList = getObjectsByList(conn, listId = detectionList, dateThreshold = flagDate, processingFlags = 0)
                else:
                    print("The list must be between 0 and 11 inclusive.  Exiting.")
                    sys.exit(1)

    print("LENGTH OF OBJECTLIST = ", len(objectList))
    currentDate = datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")
    (year, month, day, hour, min, sec) = currentDate.split(':')
    dateAndTime = "%s%s%s_%s%s%s" % (year, month, day, hour, min, sec)

    # Produce stamps with multiprocessing - try n(CPUs) threads by default
    print("Doing Stacked Diff Forced Photometry...")

    if len(objectList) > 0:
        nProcessors, listChunks = splitList(objectList, bins = int(options.numberOfThreads))

        print("%s Parallel Processing..." % (datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")))
        objectsForUpdate = parallelProcess(db, dateAndTime, nProcessors, listChunks, workerDiffStackForcedPhotometry, miscParameters = [options])
        print("%s Done Parallel Processing" % (datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")))
        if len(objectsForUpdate) > 0 and options.update:
            for row in objectsForUpdate:
                for p in row['photometry']:
                    print (row['id'], p)
                    insertStackDetRow(conn, row['id'], p)


    conn.close()

    return 0



if __name__ == '__main__':
    main()
    
