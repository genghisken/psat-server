#!/usr/bin/env python3
"""Check for Solar System moons.

Usage:
  %s <configfile> [<candidate>...] [--list=<listid>] [--matchRadius=<radius>] [--matchTimeDelta=<timedelta>] [--update] [--date=<date>] [--survey=<survey>] [--ddc] [--detectionOffset=<detectionOffet>]
  %s (-h | --help)
  %s --version

Options:
  -h --help                             Show this screen.
  --version                             Show version.
  --list=<listid>                       The object list [default: 4]
  --customlist=<customlistid>           The object custom list
  --matchRadius=<radius>                Match radius (arcsec) [default: 20]
  --matchTimeDelta=<timedelta>          Match time delta (days) [default: 0.5]
  --update                              Update the database
  --date=<date>                         Date threshold - no hyphens [default: 20130601]
  --survey=<survey>                     Survey database to interrogate [default: atlas].
  --ddc                                 Use the ddc schema (ignored if survey=panstarrs).
  --detectionOffset=<detectionOffset>   Detection offset [default: 0]

  Example:
    %s ../../../../config/config4_db4_readonly.yaml 1063629090302540900 --ddc --update
"""
import sys
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt
import os, MySQLdb, shutil, re
from gkutils.commonutils import find, Struct, cleanOptions, dbConnect, calculateRMSScatter, getAngularSeparation, coneSearchHTM, QUICK, FULL, CAT_ID_RA_DEC_COLS, PROCESSING_FLAGS
import MySQLdb
sys.path.append('../../common/python')
from queries import getATLASCandidates, getAtlasObjects, getPanSTARRSCandidates, updateTransientObservationAndProcessingStatus, insertTransientObjectComment, getObjectInfo

def updateObservationStatus(conn, objectId, status):

    rowsUpdated = 0

    try:
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)

        cursor.execute ("""
             update atlas_diff_objects
             set observation_status = %s
             where id = %s
               and (observation_status is null or (observation_status is not null and observation_status != %s))
        """, (status, objectId, status))

        rowsUpdated = cursor.rowcount

        if rowsUpdated == 0:
            print("WARNING: No transient object entries were updated.")

        cursor.close ()


    except MySQLdb.Error as e:
        print("Error %d: %s" % (e.args[0], e.args[1]))

    return rowsUpdated


def moonMatcher(conn, connCatalogues, options, candidateList, detectionOffset = 0):

    matchRadius = int(options.matchRadius)
    matchTimeDelta = float(options.matchTimeDelta)
    objectsForUpdate = []

    moon = None
    separation = None

    detectionToUse = None

    for candidate in candidateList:
        xmResult = {'id': candidate['id'], 'moon': None, 'separation': None, 'matchTime': None}

        detections = getObjectInfo(conn, candidate['id'], options.survey, options.ddc)

        counter = 0
        for det in reversed(detections):
            if options.survey == 'atlas':
                if det['dup'] >= 0:
                    if counter == detectionOffset:
                        detectionToUse = det
                        break
                    counter += 1
            elif options.survey == 'panstarrs':
                if det['psf_inst_mag'] is not None and det['psf_inst_mag_sig'] is not None and det['cal_psf_mag'] is not None:
                    if counter == detectionOffset:
                        detectionToUse = det
                        break
                    counter += 1

        # If we don't have a decent detection, skip on
        if detectionToUse is None:
            continue

        message, results = coneSearchHTM(detectionToUse['RA'], detectionToUse['DEC'], matchRadius, 'tcs_cat_satellites', queryType = FULL, conn = connCatalogues)
        if results and len(results) >= 1:
            # We have more than one object.  No we need to merge anything. Can exit now.
            for row in results:
                if detectionToUse['MJD'] > row[1]['mjd'] - matchTimeDelta and detectionToUse['MJD'] < row[1]['mjd'] + matchTimeDelta:
                    moon = row[1]['name']
                    separation = row[0]
                    xmResult['moon'] = moon
                    xmResult['separation'] = separation
                    xmResult['matchTime'] = row[1]['mjd']
                    break

        objectsForUpdate.append(xmResult)

    return objectsForUpdate


def updateObjects(conn, options, objectsForUpdate):
    for candidate in objectsForUpdate:
        mover = False
        if candidate['moon'] is not None:
            # There was a match
            mover = True
            comment = "MOONS: %s (%.2f arcsec)" % (candidate['moon'], candidate['separation'])
            print(candidate['id'], comment)

            rowsUpdated = updateTransientObservationAndProcessingStatus(conn, candidate['id'], processingFlag = PROCESSING_FLAGS['moons'], observationStatus = 'mover', survey = options.survey)

            commentRowsUpdated = insertTransientObjectComment(conn, candidate['id'], comment)
        else:
            rowsChecked = updateTransientObservationAndProcessingStatus(conn, candidate['id'], processingFlag = PROCESSING_FLAGS['moons'], survey = options.survey)

    return


def main(argv = None):
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

    candidateList = []

    detectionList = 4

    catuser = config['databases']['catalogues']['username']
    catpass = config['databases']['catalogues']['password']
    catname = config['databases']['catalogues']['database']
    cathost = config['databases']['catalogues']['hostname']

    conn = dbConnect(hostname, username, password, database, quitOnError = True)

    # 2023-03-25 KWS MySQLdb disables autocommit by default. Switch it on globally.
    conn.autocommit(True)

    connCatalogues = dbConnect(cathost, catuser, catpass, catname, quitOnError = True)

    # If the list isn't specified assume it's the Eyeball List.
    if options.list is not None:
        try:
            detectionList = int(options.list)
            if detectionList < 0 or detectionList > 8:
                print("Detection list must be between 0 and 8")
                return 1
        except ValueError as e:
            sys.exit("Detection list must be an integer")

    if options.date is not None:
        try:
            dateThreshold = '%s-%s-%s' % (options.date[0:4], options.date[4:6], options.date[6:8])
        except:
            dateThreshold = '2013-06-01'

    if options.candidate or options.list or options.customList:
        # pull from DB into a list of dicts
        if options.survey == 'atlas':
            candidateList = getATLASCandidates(conn, options, processingFlags = PROCESSING_FLAGS['moons'])
        elif options.survey == 'panstarrs':
            candidateList = getPanSTARRSCandidates(conn, options, processingFlags = PROCESSING_FLAGS['moons'])


    offset = 0
    if int(options.detectionOffset) > 0:
        offset = int(options.detectionOffset)

    objectsForUpdate = []

    if len(candidateList) > 0:
        objectsForUpdate = moonMatcher(conn, connCatalogues, options, candidateList, detectionOffset = offset)

    if len(objectsForUpdate) > 0 and options.update:
        updateObjects(conn, options, objectsForUpdate)

    return





if __name__ == '__main__':
    main()
