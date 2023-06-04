#!/usr/bin/env python
"""Crossmatch GW events using Dave's skytag code.

Usage:
  %s <configFile> <survey> [<candidate>...] [--listid=<listid>] [--customlist=<customlist>] [--datethreshold=<datethreshold>] [--timedeltas] [--distances] [--mapiteration=<mapiteration>] [--event=<event>] [--gwEventMap=<gwEventMap>] [--areaThreshold=<areaThreshold>] [--areaContour=<areaContour>] [--distanceThreshold=<distanceThreshold>] [--far=<far>] [--mapRootLocation=<mapRootLocation>] [--daysBeforeEvent=<daysBeforeEvent>] [--daysAfterEvent=<daysAfterEvent>] [--update]
  %s (-h | --help)
  %s --version

  Survey must be panstarrs | atlas

Options:
  -h --help                                Show this screen.
  --version                                Show version.
  --listid=<listid>                        List ID [default: 4].
  --customlist=<customlist>                Custom list ID.
  --datethreshold=<datethreshold>          Only pull out objects with a flag date greater than the datethreshold in YYYMMDD format. Only applied to listid [defaut: 20230101].
  --timedeltas                             Pull out the earliest MJD (if present) for time delta calculations.
  --distances                              Add the individual distances to the results.
  --mapiteration=<mapiteration>            By default code tries to derive the map iteration from the directory name. Override this.
  --event=<event>                          GW event. Override a database check of events.
  --gwEventMap=<gwEventMap>                GW event map. Override the map location derived from the database.
  --areaThreshold=<areaThreshold>          Area threshold before doing the crossmatch, ignored if map provided [default: 2000].
  --areaContour=<areaContour>              Area contour to which to apply the area check - 90, 50 or 10, ignored if map provided [default: 90].
  --distanceThreshold=<distanceThreshold>  Distance threshold to apply if present (Mpc).
  --mapRootLocation=<mapRootLocation>      Root location where the maps are stored [default: /data/psdb3data1/o4_events].
  --daysBeforeEvent=<daysBeforeEvent>      Days before the event to trigger search [default: 10].
  --daysAfterEvent=<daysAfterEvent>        Days after the event to trigger search [default: 21].
  --far=<far>                              False alarm rate (e.g. 1 in 6 months) [default: 0.000000015844369].
  --update                                 Update the database.


Example:
  %s /tmp/config.yaml --event=MS230506p --gwEventMap=/data/psdb3data1/o4_events/mockevents/MS230506p/20230506T160233_initial/bayestar.multiorder.fits --list=2
  %s /tmp/config.yaml --event=MS230506p --gwEventMap=/data/psdb3data1/o4_events/mockevents/MS230506p/20230506T160233_initial/bayestar.multiorder.fits panstarrs --list=5 --timedeltas
  %s ../../../../../atlas/config/config4_db1_readonly.yaml --event=S230518h --gwEventMap=/data/psdb3data1/o4_events/superevents/S230518h/20230526T221627_update/ligo-skymap-from-samples.multiorder.fits atlas --listid=10 --update --timedeltas --datethreshold=20230517
  %s ../../../../../atlas/config/config4_db1_readonly.yaml atlas --listid=2 --update --timedeltas --distanceThreshold=500
"""
import sys
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt
import os
from gkutils.commonutils import Struct, cleanOptions, dbConnect, splitList, parallelProcess, getCurrentMJD, getDateFromMJD

# Need to set the PYTHONPATH to find the stamp downloader. These queries should be moved to
# a common utils package. These are also ATLAS only. Need equivalents for Pan-STARRS.
from pstamp_utils import getATLASObjectsByList, getATLASObjectsByCustomList
from pstamp_utils import getObjectsByList as getPSObjectsByList
from pstamp_utils import getObjectsByCustomList as getPSObjectsByCustomList

from skytag.commonutils import prob_at_location
import glob
from math import isinf


def deleteObjectGWInfo(conn, event, objectId):
    import MySQLdb

    try:
        cursor = conn.cursor (MySQLdb.cursors.DictCursor)

        cursor.execute ("""
            delete from tcs_gravity_event_annotations
             where gravity_event_id = %s
               and map_name = %s
               and transient_object_id = %s
               and map_iteration = %s
            """, (event['superevent_id'], os.path.basename(event['map']), objectId, event['map_iteration'],))

    except MySQLdb.Error as e:
        sys.stderr.write("Error %d: %s\n" % (e.args[0], e.args[1]))

    cursor.close ()
    conn.commit()
    return


def insertObjectGWInfo(conn, event, objectId, contour, timeDelta = None, probability = None, distance = None, distanceSigma = None):
    import MySQLdb

    try:
        cursor = conn.cursor (MySQLdb.cursors.DictCursor)

        cursor.execute ("""
             insert into tcs_gravity_event_annotations (gravity_event_id,
                                                        gracedb_id,
                                                        map_name,
                                                        map_iteration,
                                                        transient_object_id,
                                                        enclosing_contour,
                                                        days_since_event,
                                                        probability,
                                                        distance,
                                                        distance_sigma,
                                                        dateLastModified,
                                                        updated,
                                                        dateCreated)
             values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), 0, now())
             """, (event['superevent_id'], event['superevent_id'], os.path.basename(event['map']), event['map_iteration'], objectId, contour, timeDelta, probability, distance, distanceSigma))

    except MySQLdb.Error as e:
        sys.stderr.write("Error %d: %s\n" % (e.args[0], e.args[1]))

    cursor.close ()
    return conn.insert_id()

def writeGWTags(conn, options, objectDict, event):
    mapIteration = event['map_iteration']

    inserts = []
    for objectId, prob in objectDict.items():
        if prob[0] <= 90.0:
            # Only write the data if the object lies within the 90% region.
            print(objectId, prob)
            deleteObjectGWInfo(conn, event, objectId)
            insertId = insertObjectGWInfo(conn, event, objectId, prob[-1], timeDelta = prob[1], probability = prob[0], distance = prob[2][0], distanceSigma = prob[2][1])
            inserts.append(insertId)
    return inserts

def getContour(prob):
    # Subtract a very small amount so that objects with prob 20.0 appear in the 20 contour bin, not 30.
    contour = (int((prob-0.0000001)/10)+1)*10
    return contour


# Get the currently active initial and update events.
def getActiveEvents(conn):
    import MySQLdb

    try:
        cursor = conn.cursor (MySQLdb.cursors.DictCursor)

        cursor.execute ("""
            select superevent_id,
                   significant,
                   far,
                   mjd_obs,
                   area90,
                   area50,
                   area10,
                   distmean,
                   diststd,
                   map_iteration,
                   map
              from tcs_gravity_alerts
             where alert_time in (select max(alert_time) as latest_alert
                                    from tcs_gravity_alerts
                                group by superevent_id)
               and alert_type in ('INITIAL', 'UPDATE')
        """)
        resultSet = cursor.fetchall ()

        cursor.close ()

    except MySQLdb.Error as e:
        print("Error %d: %s" % (e.args[0], e.args[1]))

    return resultSet


def getFiles(regex, directory, recursive = False):

    directoryRecurse = '/'
    if recursive:
        directoryRecurse = '/**/'
    fileList = glob.glob(directory + directoryRecurse + regex, recursive=recursive)

    fileList.sort()

    return fileList


# The event maps are not always named consistently!
def getEventMap(mapIteration, options):
    gwMap = None

    maps = getFiles(mapIteration + '/*.fits', options.mapRootLocation, recursive = True)

    if maps and len(maps) == 1:
        gwMap = maps[0]

    if maps and len(maps) > 1:
        # What do we do if we get more than one map? Just choose the zeroth map.
        gwMap = maps[0]
        
    return gwMap

def getActiveSupereventInformation(conn, options):
    todayMJD = getCurrentMJD()
    activeSuperevents = []
    # Get the active superevents from the database
    events = getActiveEvents(conn)
    for event in events:
        if event['area' + options.areaContour] < float(options.areaThreshold) \
                and todayMJD > event['mjd_obs'] - float(options.daysBeforeEvent) \
                and todayMJD < event['mjd_obs'] + float(options.daysAfterEvent) \
                and ((options.distanceThreshold is not None and event ['distmean'] < float(options.distanceThreshold)) or options.distanceThreshold is None) \
                and event['far'] < float(options.far) \
                and event['significant']:
            # get the map, if we can find one.
            if event['map_iteration'] is not None and event['map'] is None:
                mapLocation = getEventMap(event['map_iteration'], options)
                if mapLocation:
                    event['map'] = mapLocation
                    event['dateThreshold'] = getDateFromMJD(event['mjd_obs'] - float(options.daysBeforeEvent))
                    activeSuperevents.append(event)
            else:
                # There is a map specified in the database.
                event['dateThreshold'] = getDateFromMJD(event['mjd_obs'] - float(options.daysBeforeEvent))
                activeSuperevents.append(event)
            print("Adding event %s to crossmatch." % event['superevent_id'])
        else:
            print("Event %s fails the filter test." % event['superevent_id'])
    return activeSuperevents


def crossmatchGWEvents(conn, options, event):

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
                if int(options.listid) >= 0 and int(options.listid) < 11:
                    detectionList = int(options.listid)
                    if options.survey == 'panstarrs':
                        objectList = getPSObjectsByList(conn, listId = detectionList, processingFlags = 0, dateThreshold = event['dateThreshold'])
                        #objectList = getPSObjectsByList(conn, listId = detectionList, processingFlags = 0)
                    else:
                        objectList = getATLASObjectsByList(conn, listId = detectionList, processingFlags = 0, dateThreshold = event['dateThreshold'])
                        #objectList = getATLASObjectsByList(conn, listId = detectionList, processingFlags = 0)
                else:
                    print("The list must be between 0 and 9 inclusive.  Exiting.")
                    sys.exit(1)

    print("LENGTH OF OBJECTLIST = ", len(objectList))
    if len(objectList) == 0:
        print("No objects to check")
        print(dateThreshold)
        exit(0)

    requestDistance = False
    if options.distances:
        requestDistance = True

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
        mapPath=event['map'],
        distance = requestDistance)

    # prob_at_location returns a simple list if timedeltas not requested, otherwise a list of lists.
    # Create a consistent two element array which contains None if timedeltas not requested.
    if not options.timedeltas and not options.distances:
        deltas = [None] * len(probs[0])
        distances = [(None, None)] * len(probs[0])
        probs = (probs[0], deltas, distances)
    elif options.timedeltas and not options.distances:
        distances = [(None, None)] * len(probs[0])
        probs = [probs[0], probs[1], distances]
    elif not options.timedeltas and options.distances:
        deltas = [None] * len(probs[0])
        probs = [probs[0], deltas, probs[1]]

    # The last part of the array should be distances. Let's check for infinity and overwrite in place if true.
    if options.distances:
        distances = probs[-1]
        for i,d in enumerate(distances):
            if isinf(d[0]):
                distances[i] = (None, None)

    contours = [getContour(prob) for prob in probs[0]]

    objectProbs = {}
    if len(probs[0]) == len(ids):
        for id, prob, delta, dist, contour in zip(ids, probs[0], probs[1], probs[2], contours):
            objectProbs[id] = [prob, delta, dist, contour]
    else:
        print("Probs and input IDs are not the same length. Cannot continue.")

    inserts = []

    # Now write the data into the database.
    if options.update and objectProbs:
        inserts = writeGWTags(conn, options, objectProbs, event)

    print ("%d inserts written to the database." % len(inserts))

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

    import yaml
    with open(options.configFile) as yaml_file:
        config = yaml.safe_load(yaml_file)

    username = config['databases']['local']['username']
    password = config['databases']['local']['password']
    database = config['databases']['local']['database']
    hostname = config['databases']['local']['hostname']


    conn = dbConnect(hostname, username, password, database)
    conn.autocommit(True)

    dateThreshold = '2023-01-01'

    if options.datethreshold is not None:
        try:
            dateThreshold = '%s-%s-%s' % (options.datethreshold[0:4], options.datethreshold[4:6], options.datethreshold[6:8])
        except:
            dateThreshold = '2023-01-01'

    activeSuperevents = []

    if options.gwEventMap is not None and options.event is not None:
        # Use the manual parameters we were handed as options.
        if options.mapiteration is not None:
            mapIteration = options.mapiteration
        else:
            # Derive the map iteration from the parent directory.
            mapIteration = os.path.basename(os.path.dirname(options.gwEventMap))
        activeSuperevents = [{'superevent_id': options.event, 'map': options.gwEventMap, 'dateThreshold': dateThreshold, 'map_iteration': mapIteration}]
        
    if options.gwEventMap is None or options.event is None:
        activeSuperevents = getActiveSupereventInformation(conn, options)

    if len(activeSuperevents) > 0:
        for event in activeSuperevents:
            #print(event)
            crossmatchGWEvents(conn, options, event)

    conn.close()


if __name__ == '__main__':
    main()
