#!/usr/bin/env python
"""Refactored post ingest cut code for ATLAS.

Usage:
  %s <configfile> [<candidate>...] [--datethreshold=<datethreshold>] [--update] [--recent] [--loglocation=<loglocation>] [--logprefix=<logprefix>] [--logprefixlargedataset=<logprefixlargedataset>] [--logprefixeliminate=<logprefixeliminate>]
  %s (-h | --help)
  %s --version

Options:
  -h --help                                           Show this screen.
  --version                                           Show version.
  --datethreshold=<datethreshold>                     Date threshold - no hyphens [default: 20170925].
  --update                                            Update the database.
  --recent                                            Check for recent objects.
  --loglocation=<loglocation>                         Log file location [default: /tmp/]
  --logprefix=<logprefix>                             Log prefix [default: applyATLASCuts]
  --logprefixlargedataset=<logprefixlargedataset>     Log prefix [default: applyATLASCutsLargeDataSet]
  --logprefixeliminate=<logprefixeliminate>           Log prefix [default: applyATLASCutsEliminate]


  Example:
    %s ../../../../../atlas/config/config4_db5_readonly.yaml 1063629090302540900 --update
"""


import sys
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt

import multiprocessing, sys, os, MySQLdb, time, datetime, logging
from gkutils.commonutils import dbConnect, splitList, parallelProcess, cleanOptions, Struct
from postIngestAtlasCutsDDC import getAtlasObjectsToCheck, getMaxFollowupId, promoteObjectToEyeballList, applyAtlasDiffCuts, getMostRecentProcessedDate, checkIsObjectAlreadyPromoted, getSiteMasks

import numpy as n

# The Queue object is required for the exceptions that are NOT defined in multiprocessing.Queue
import queue

# Allow random shuffling of lists.
import random


def worker(num, db, objectListFragment, dateAndTime, firstPass, miscParameters, q):
    """thread worker function"""
    # Redefine the output to be a log file.
    options = miscParameters[0]
    recent = miscParameters[1]
    siteMasks = miscParameters[2]

    sys.stdout = open('%s%s_%s_%d.log' % (options.loglocation, options.logprefix, dateAndTime, num), "w")
    conn = None
    try:
        conn = dbConnect(db['hostname'], db['username'], db['password'], db['database'], quitOnError = True)
        conn.autocommit(True)
    except:
        print("Cannot connect to the local database. Terminating this process.")
        q.put([])
        return 0

    objectsForUpdate = applyAtlasDiffCuts(conn, dateAndTime, objectListFragment, recent = recent, masks = siteMasks)

    # Write the objects for update onto a Queue object
    print("Adding %d objects onto the queue." % len(objectsForUpdate))

    q.put(objectsForUpdate)

    print("Process complete.")
    conn.close()
    print("DB Connection Closed - exiting")

    return 0



def getLargeDataSet(num, db, objectListFragment, dateAndTime, firstPass, miscParameters, q):
    options = miscParameters[0]
    dateThreshold = miscParameters[1]

    sys.stdout = open('%s%s_%s_%d.log' % (options.loglocation, options.logprefixlargedataset, dateAndTime, num), "w")
    conn = dbConnect(db['hostname'], db['username'], db['password'], db['database'], quitOnError = True)
    conn.autocommit(True)

    print(dateThreshold)
    objectsForUpdate = getAtlasObjectsToCheck(conn, dateThreshold = dateThreshold)

    # Write the objects for update onto a Queue object
    print("Adding %d objects onto the queue." % len(objectsForUpdate))

    q.put(objectsForUpdate)

    print("Process complete.")
    conn.close()
    print("DB Connection Closed - exiting")

    return 0


def eliminatePreviouslyPromotedObjects(num, db, objectListFragment, dateAndTime, firstPass, miscParameters, q):
    options = miscParameters[0]
    sys.stdout = open('%s%s_%s_%d.log' % (options.loglocation, options.logprefixeliminate, dateAndTime, num), "w")
    conn = dbConnect(db['hostname'], db['username'], db['password'], db['database'], quitOnError = True)
    conn.autocommit(True)

    objectsForUpdate = checkIsObjectAlreadyPromoted(conn, objectListFragment)

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

    # Use utils.Struct to convert the dict into an object for compatibility with old optparse code.
    options = Struct(**opts)

    import yaml
    with open(options.configfile) as yaml_file:
        config = yaml.safe_load(yaml_file)

    username = config['databases']['local']['username']
    password = config['databases']['local']['password']
    database = config['databases']['local']['database']
    hostname = config['databases']['local']['hostname']

    db = {'username': username,
          'password': password,
          'database': database,
          'hostname': hostname}

    conn = dbConnect(hostname, username, password, database, quitOnError = True)
    conn.autocommit(True)

    # Get the date for the log files.
    currentDate = datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")
    (year, month, day, hour, min, sec) = currentDate.split(':')
    dateAndTime = "%s%s%s_%s%s%s" % (year, month, day, hour, min, sec)
    sqlFlagDate = "%s-%s-%s %s:%s:%s" % (year, month, day, hour, min, sec)
    sqlDateTime = "%s-%s-%s %s:%s:%s" % (year, month, day, hour, min, sec)

    # 2022-09-07 KWS Read the heatmaps for each site.
    multiplier = 1.5
    siteMasks = getSiteMasks(conn, multiplier)

    conn.close()

    objectsForUpdate = []

    if len(options.candidate) > 0:
        for candidate in options.candidate:
            try:
                objectsForUpdate.append(candidate)
            except ValueError:
                print("Object IDs must be integers")
                sys.exit(1)

    else:
        # Get only the ATLAS objects that don't have the 'moons' flag set.
        dateThreshold = getMostRecentProcessedDate(conn)

        # When both thresholds are defined, use the one in the database.
        if options.datethreshold is not None and dateThreshold is not None:
            pass
        else:
            try:
                dateThreshold = '%s-%s-%s 00:00:00' % (options.datethreshold[0:4], options.datethreshold[4:6], options.datethreshold[6:8])
            except:
                # Something went wrong parsing the datethreshold - just use the start of today.
                dateThreshold = sqlFlagDate

        print(dateThreshold, sqlFlagDate)

        # 2013-07-31 KWS The number of objects is so large that we need to minimise memory consumption by spawning
        #                a process and grabbing the results. The memory is released by the spawned process once we're
        #                finished with it.
        objectsForUpdate = parallelProcess(db, dateAndTime, 1, [0], getLargeDataSet, miscParameters = [options, dateThreshold], firstPass = True)



    # We no longer update the quality_threshold_pass flag.  It takes far too long.
    # We check it for each object recurrence on-the-fly.


    #conn.close()

    # NOTE! It is VITAL that the objects are ordered by ID (primary key).  If not, the I/O delay becomes
    #       unacceptable!  No point trying to randomize the list.

    print("TOTAL OBJECTS TO FILTER = %d" % len(objectsForUpdate))
    if len(objectsForUpdate) > 0:
        nProcessors, listChunks = splitList(objectsForUpdate, bins=28)

        print("%s Parallel Processing..." % (datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")))
        objectsForUpdate = parallelProcess(db, dateAndTime, nProcessors, listChunks, eliminatePreviouslyPromotedObjects, miscParameters = [options], firstPass = True)
        print("%s Done Parallel Processing" % (datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")))

        print("Length of list is:", len(objectsForUpdate))

    print("TOTAL OBJECTS TO CHECK = %d" % len(objectsForUpdate))
    # Don't do anything unless there is at least one request!
    if len(objectsForUpdate) > 0:
        nProcessors, listChunks = splitList(objectsForUpdate, bins=28)

        print("%s Parallel Processing..." % (datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")))
        updateList = parallelProcess(db, dateAndTime, nProcessors, listChunks, worker, miscParameters = [options, options.recent, siteMasks], firstPass = True)
        print("%s Done Parallel Processing" % (datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")))

        # 2012-03-23 KWS Re-connect to the DB.  Only reconnect when we need to do updates.
        conn = dbConnect(hostname, username, password, database, quitOnError = True)
        # 2023-03-13 KWS Need to set autocommit for all transactions with InnoDB tables. Set to False by default.
        conn.autocommit(True)

        # Grab highest followup ID.  MASTER PROCESS
        maxFollowupId = getMaxFollowupId(conn)

        print("")
        print("Max followup ID = %d" % maxFollowupId)

        objectsPromoted = 0
        followupId = maxFollowupId + 1

        print("%s Updating..." % (datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")))

        for object in updateList:
           if object["promote"]:
              if options.update:
                  promoteObjectToEyeballList(conn, object["id"], followupId, sqlFlagDate)
              followupId += 1
              objectsPromoted += 1

        print("%s Done Updating" % (datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")))

        print("TOTAL OBJECTS PROMOTED = %d" % objectsPromoted)

        conn.close()

    else:
        print("No objects to process.")


if __name__=="__main__":
     main()


