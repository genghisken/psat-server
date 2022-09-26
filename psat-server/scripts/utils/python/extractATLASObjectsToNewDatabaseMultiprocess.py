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
  %s <username> <password> <database> <hostname> <sourceschema> [<candidates>...] [--truncate] [--ddc] [--list=<listid>] [--flagdate=<flagdate>] [--copyimages] [--loglocation=<loglocation>] [--logprefix=<logprefix>]
  %s (-h | --help)
  %s --version

Options:
  -h --help                       Show this screen.
  --version                       Show version.
  --list=<listid>                 List to be migrated [default: 2].
  --flagdate=<flagdate>           Flag date threshold beyond which we will select objects [default: 20170920].
  --ddc                           Assume the DDC schema.
  --truncate                      Truncate the database tables. Default is NOT to truncate.
  --copyimages                    Copy the images as well (extremely time consuming).
  --loglocation=<loglocation>     Log file location [default: /tmp/]
  --logprefix=<logprefix>         Log prefix [default: migration]


"""
import sys
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt
import os, MySQLdb, shutil, re
from gkutils.commonutils import dbConnect, find, Struct, cleanOptions
from psat_server_web.atlas.atlas.commonqueries import getNonDetectionsUsingATLASFootprint, LC_POINTS_QUERY_ATLAS_DDC, ATLAS_METADATADDC, filterWhereClauseddc, FILTERS
from extractATLASObjectsToNewDatabase import getATLASObjects, getSpecifiedObjects, migrateData
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

    connPrivateReadonly = dbConnect(options.hostname, options.username, options.password, options.sourceschema)
    if not connPrivateReadonly:
        print("Cannot connect to the private database")
        return 1

    migrateData(conn, connPrivateReadonly, listFragment, options.database, options.sourceschema, truncateTables = options.truncate, ddc = options.ddc, copyImages = options.copyimages)

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

    detectionList = 2
    if options.list is not None:
        try:
            detectionList = int(options.list)
            if detectionList < 1 or detectionList > 10:
                print("Detection list must be between 1 and 10")
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

    objectList = []

    conn = dbConnect(options.hostname, options.username, options.password, options.database)
    if not conn:
        print("Cannot connect to the public database")
        return 1

    connPrivateReadonly = dbConnect(options.hostname, options.username, options.password, options.sourceschema)
    if not connPrivateReadonly:
        print("Cannot connect to the private database")
        return 1

    # Supplied candidates override the specified list
    if options.candidates:
        for candidate in options.candidates:
            candidateList.append(int(candidate))
        candidateList = getSpecifiedObjects(connPrivateReadonly, candidateList)

    else:
        candidateList = getATLASObjects(connPrivateReadonly, listId = detectionList)

    print("Length of list = %d)" % len(candidateList))

    currentDate = datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")
    (year, month, day, hour, min, sec) = currentDate.split(':')
    dateAndTime = "%s%s%s_%s%s%s" % (year, month, day, hour, min, sec)

    if len(objectList) > 0:
        nProcessors, listChunks = splitList(objectList)

        print("%s Parallel Processing..." % (datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")))
        parallelProcess(db, dateAndTime, nProcessors, listChunks, workerStampCutter, miscParameters = [options], drainQueues = False)
        print("%s Done Parallel Processing" % (datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")))


    conn.close ()
    connPrivateReadonly.close ()

    return 0


if __name__ == '__main__':
    main()
