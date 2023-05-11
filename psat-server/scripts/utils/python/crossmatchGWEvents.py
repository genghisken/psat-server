#!/usr/bin/env python
"""Crossmatch GW events using Dave's skytag code.

Usage:
  %s <configFile> <event> <gwEventMap> <survey> [<candidate>...] [--listid=<listid>] [--customlist=<customlist>] [--datethreshold=<datethreshold>] [--timedeltas] [--update]
  %s (-h | --help)
  %s --version

  Survey must be panstarrs | atlas

Options:
  -h --help                         Show this screen.
  --version                         Show version.
  --listid=<listid>                 List ID [default: 4].
  --customlist=<customlist>         Custom list ID.
  --datethreshold=<datethreshold>   Only pull out objects with a flag date greater than the datethreshold in YYYMMDD format. Only applied to listid.
  --timedeltas                      Pull out the earliest MJD (if present) for time delta calculations.
  --update                          Update the database.


Example:
  %s /tmp/config.yaml MS230506p /data/psdb3data1/o4_events/mockevents/MS230506p/20230506T160233_initial/bayestar.multiorder.fits --list=2
  %s /tmp/config.yaml MS230506p /data/psdb3data1/o4_events/mockevents/MS230506p/20230506T160233_initial/bayestar.multiorder.fits panstarrs --list=5 --timedeltas
"""
import sys
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt
import os
from gkutils.commonutils import Struct, cleanOptions, dbConnect, splitList, parallelProcess

# Need to set the PYTHONPATH to find the stamp downloader. These queries should be moved to
# a common utils package. These are also ATLAS only. Need equivalents for Pan-STARRS.
from pstamp_utils import getATLASObjectsByList, getATLASObjectsByCustomList
from pstamp_utils import getObjectsByList as getPSObjectsByList
from pstamp_utils import getObjectsByCustomList as getPSObjectsByCustomList

from skytag.commonutils import prob_at_location


def deleteObjectGWInfo(conn, options, objectId):
    import MySQLdb

    try:
        cursor = conn.cursor (MySQLdb.cursors.DictCursor)

        cursor.execute ("""
            delete from tcs_gravity_event_annotations
             where gravity_event_id = %s
               and map_name = %s
               and transient_object_id = %s
            """, (options.event, os.path.basename(options.gwEventMap), objectId,))

    except MySQLdb.Error as e:
        sys.stderr.write("Error %d: %s\n" % (e.args[0], e.args[1]))

    cursor.close ()
    conn.commit()
    return


def insertObjectGWInfo(conn, options, objectId, contour, timeDelta = None, probability = None):
    import MySQLdb

    try:
        cursor = conn.cursor (MySQLdb.cursors.DictCursor)

        cursor.execute ("""
             insert into tcs_gravity_event_annotations (gravity_event_id,
                                                        gracedb_id,
                                                        map_name,
                                                        transient_object_id,
                                                        enclosing_contour,
                                                        days_since_event,
                                                        probability,
                                                        dateLastModified,
                                                        updated,
                                                        dateCreated)
             values (%s, %s, %s, %s, %s, %s, %s, now(), 0, now())
             """, (options.event, options.event, os.path.basename(options.gwEventMap), objectId, contour, timeDelta, probability))

    except MySQLdb.Error as e:
        sys.stderr.write("Error %d: %s\n" % (e.args[0], e.args[1]))

    cursor.close ()
    return conn.insert_id()

def writeGWTags(conn, options, objectDict):
    inserts = []
    for objectId, prob in objectDict.items():
        if prob[0] <= 90.0:
            # Only write the data if the object lies within the 90% region.
            deleteObjectGWInfo(conn, options, objectId)
            insertId = insertObjectGWInfo(conn, options, objectId, prob[2], timeDelta = prob[1], probability = prob[0])
            inserts.append(insertId)
    return inserts

def getContour(prob):
    # Subtract a very small amount so that objects with prob 20.0 appear in the 20 contour bin, not 30.
    contour = (int((prob-0.0000001)/10)+1)*10
    return contour

def crossmatchGWEvents(options):

    import yaml
    with open(options.configFile) as yaml_file:
        config = yaml.safe_load(yaml_file)

    username = config['databases']['local']['username']
    password = config['databases']['local']['password']
    database = config['databases']['local']['database']
    hostname = config['databases']['local']['hostname']


    conn = dbConnect(hostname, username, password, database)
    conn.autocommit(True)

    objectList = []

    if options.candidate is not None and len(options.candidate) > 0:
        for cand in options.candidate:
            if options.survey == 'panstarrs':
                obj = getPSObjectsByList(conn, objectId = int(cand))
                if obj:
                    objectList.append(obj)
            else:
                obj = getATLASObjectsByList(conn, objectId = int(cand))
                if obj:
                    objectList.append(obj)

    else:

        if options.customlist is not None:
            if int(options.customlist) > 0 and int(options.customlist) < 100:
                customList = int(options.customlist)
                if options.survey == 'panstarrs':
                    objectList = getPSObjectsByCustomList(conn, customList, processingFlags = 0)
                else:
                    objectList = getATLASObjectsByCustomList(conn, customList, processingFlags = 0)
            else:
                print("The list must be between 1 and 100 inclusive.  Exiting.")
                sys.exit(1)
        else:
            if options.listid is not None:
                if int(options.listid) >= 0 and int(options.listid) < 9:
                    detectionList = int(options.listid)
                    if options.survey == 'panstarrs':
                        objectList = getPSObjectsByList(conn, listId = detectionList, processingFlags = 0)
                    else:
                        objectList = getATLASObjectsByList(conn, listId = detectionList, processingFlags = 0)
                else:
                    print("The list must be between 0 and 9 inclusive.  Exiting.")
                    sys.exit(1)

    print("LENGTH OF OBJECTLIST = ", len(objectList))

    ids = []
    ras = []
    decs = []
    mjds = []

    for candidate in objectList:
        if options.timedeltas:
            if candidate['earliest_mjd']:
                ids.append(candidate['id'])
                ras.append(candidate['ra'])
                decs.append(candidate['dec'])
                mjds.append(candidate['earliest_mjd'])
        else:
            ids.append(candidate['id'])
            ras.append(candidate['ra'])
            decs.append(candidate['dec'])

    probs = prob_at_location(
        ra=ras,
        dec=decs,
        mjd=mjds,
        mapPath=options.gwEventMap)


    # prob_at_location returns a simple list if timedeltas not requested, otherwise a list of lists.
    # Create a consistent two element array which contains None if timedeltas not requested.
    if not options.timedeltas:
        deltas = [None] * len(probs)
        probs = (probs, deltas)

    contours = [getContour(prob) for prob in probs[0]]

    objectProbs = {}
    if len(probs[0]) == len(ids):
        for id, prob, delta, contour in zip(ids, probs[0], probs[1], contours):
            objectProbs[id] = [prob, delta, contour]
    else:
        print("Probs and input IDs are not the same length. Cannot continue.")

    print(objectProbs)

    inserts = []

    # Now write the data into the database.
    if options.update and objectProbs:
        inserts = writeGWTags(conn, options, objectProbs)

    print ("%d inserts written to the database." % len(inserts))
    conn.close()


    return objectProbs

def main(argv = None):
    """main.

    Args:
        argv:
    """
    opts = docopt(__doc__, version='0.1')
    opts = cleanOptions(opts)

    # Use utils.Struct to convert the dict into an object for compatibility with old optparse code.
    options = Struct(**opts)
    crossmatchGWEvents(options)


if __name__ == '__main__':
    main()
