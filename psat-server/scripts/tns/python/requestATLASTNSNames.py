#!/usr/bin/env python
"""Get ATLAS TNS names.

Usage:
  %s <configfile> [<objectid>...] [--list=<list>] [--customlist=<customlist>] [--getreports] [--live] [--ddc] [--daysbefore=<daysbefore>] [--daysafter=<daysafter>] [--internalids] [--donotsend]
  %s (-h | --help)
  %s --version

Options:
  -h --help                   Show this screen.
  --version                   Show version.
  --list=<list>               Grab objects from the specified list.
  --customlist=<customlist>   Grab objects from a custom list.
  --update                    Update the database
  --getreports                Get reports from the TNS
  --live                      Send to the LIVE TNS. By default reports go to the Sandbox.
  --ddc                       Use the ddc schema
  --daysbefore=<daysbefore>   Days before flag date [default: 4.0].
  --daysafter=<daysafter>     Days after flag date [default: 20.0].
  --internalids               Add the new internal_ids key as custom key/value pair dictionary
  --donotsend                 Do not send a report to TNS. Just test.

Example:

"""

import sys
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt

import json
import time
import logging

from tnsUtils import tnsAddRequestToDatabase, tnsUpdateRequestStatus, tnsUpdateRequestDownloadAttempts, tnsGetRequestList, getSubmissionReports
from gkutils.commonutils import dbConnect, PROCESSING_FLAGS, calculateRMSScatter, getDateFractionMJD, coneSearchHTM, QUICK
sys.path.append('../../common/python')
from queries import getObjectInfo
# 2018-05-21 KWS Retest the cut to grab the flag MJD
from postIngestAtlasCutsDDC_XGBOOST_MJD_WINDOW import testObject

# Use the TNS logger /tmp/tns.log to record log info
logger = logging.getLogger(__name__)

# 2025-07-07 KWS Added new ATLAS-TDO definitions.
ATLASFilters = {'c': '71', 'o': '72', 'w': '73'}
ATLASInstrument = {'02a': '159', '01a': '160', '03a': '255', '04a': '256', '05r': '290'}
ATLASGroup = 18
TNSAUTHORS = "J. Tonry, L. Denneau, A. Heinze, H. Weiland, H. Flewelling (IfA, University of Hawaii), B. Stalder (LSST), A. Rest (STScI), C. Stubbs (Harvard University), K. W. Smith, S. J. Smartt, D. R. Young, K. Maguire, S. Prentice, O. McBrien, D. O'Neill, P. Clark, M. Magee, M. Fulton, A. McCormack (Queen's University Belfast), D. E. Wright (University of Minnesota)"

# A selection of possible responses.  We currently only use 1 and 2.
SUBMITTED           = 1
COMPLETE            = 2
COMMUNICATION_ERROR = 3
TIMEOUT             = 4
DOWNLOADING         = 5
CORRUPT             = 7

# 2017-10-24 KWS Do NOT attempt to report an object that does
#                not yet have an ATLAS name. (E.g. Something in
#                the eyeball list can accidentally get added to
#                the list of objects to report without being
#                promoted.)
# 2018-06-07 KWS Stop known minor planets from being registered on TNS.
#                (E.g. Pluto!)
def getObjectsByList(conn, listId = 2, objectType = -1, dateThreshold = '2018-01-31', processingFlags = PROCESSING_FLAGS['tns']):
    """
    Get the ATLAS objects we want to register by object list

    :param conn: database connection
    :param listId:  (Default value = 4)
    :param objectType:  (Default value = -1)
    :param dateThreshold:  (Default value = '2009-01-01')
    :return resultSet: The tuple of object dicts

    """
    import MySQLdb

    try:
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)

        cursor.execute ("""
            select o.id, ifnull(s.ra_avg, o.ra) ra, ifnull(s.dec_avg, o.`dec`) `dec`, o.id 'name', followup_flag_date, atlas_designation
            from atlas_diff_objects o
            left join tcs_latest_object_stats s on s.id = o.id
            where detection_list_id = %s
            and (processing_flags & %s = 0 or processing_flags is null)
            and followup_flag_date > %s
            and atlas_designation is not null
            and atlas_designation != ''
            and observation_status != 'mover'
            order by followup_id
        """, (listId, processingFlags, dateThreshold))

        resultSet = cursor.fetchall ()
        cursor.close ()

    except MySQLdb.Error as e:
        sys.stderr.write("Error %d: %s\n" % (e.args[0], e.args[1]))
        sys.exit (1)

    return resultSet

def getObjectById(conn, objectId):
    """
    Get the ATLAS objects we want to register by object list

    :param conn: database connection
    :param objectId:  The atlas object ID
    :return resultSet: The object dict

    """
    import MySQLdb

    try:
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)

        cursor.execute ("""
            select o.id, ifnull(s.ra_avg, o.ra) ra, ifnull(s.dec_avg, o.`dec`) `dec`, o.id 'name', followup_flag_date, atlas_designation
            from atlas_diff_objects o
            left join tcs_latest_object_stats s on s.id = o.id
            where o.id = %s
            and atlas_designation is not null
            and atlas_designation != ''
        """, (objectId,))

        resultSet = cursor.fetchone ()
        cursor.close ()

    except MySQLdb.Error as e:
        sys.stderr.write("Error %d: %s\n" % (e.args[0], e.args[1]))
        sys.exit (1)

    return resultSet


# 2018-06-07 KWS Stop known minor planets from being registered on TNS.
#                (E.g. Pluto!)
def getObjectsByCustomList(conn, customList, objectType = -1, processingFlags = PROCESSING_FLAGS['tns']):
    """

    :param conn: database connection
    :param customList: 
    :param objectType:  (Default value = -1)
    :return resultSet: The tuple of object dicts

    """
    import MySQLdb

    try:
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute ("""
            select o.id, o.ra, o.`dec`, o.id 'name', o.followup_flag_date, o.atlas_designation
            from atlas_diff_objects o, tcs_object_groups g
            where g.object_group_id = %s
              and o.observation_status != 'mover'
              and g.transient_object_id = o.id
              and (o.processing_flags & %s = 0 or o.processing_flags is null)
            order by o.followup_id
        """, (customList, processingFlags))

        resultSet = cursor.fetchall ()
        cursor.close ()

    except MySQLdb.Error as e:
        sys.stderr.write("Error %d: %s\n" % (e.args[0], e.args[1]))
        sys.exit (1)

    return resultSet


def updateTNSRequestFlag(conn, objectList, processingFlag = PROCESSING_FLAGS['tns']):
    """
    Update the processing flag for the relevant database object to prevent
    repeat processing of the same objects.

    :param conn: database connection
    :param objectList: 
    :param processingFlag:  (Default value = PROCESSING_FLAGS['tns'])
    :return updatedRows: The number of rows updated

    """
    import MySQLdb

    updatedRows = 0

    try:
        cursor = conn.cursor (MySQLdb.cursors.DictCursor)

        for transient in objectList:
            cursor.execute ("""
                update atlas_diff_objects
                set processing_flags = if(processing_flags is null, %s, processing_flags | %s)
                where id = %s
                """, (processingFlag, processingFlag, transient['id']))

        updatedRows = cursor.rowcount
        cursor.close()

    except MySQLdb.Error as e:
        sys.stderr.write("Error %d: %s\n" % (e.args[0], e.args[1]))

    return updatedRows


def updateTNSNames(conn, tnsNames):
    """
    Depending on what we got back from the nameserver, update the appropriate field
    in the database for the relevant object.

    :param conn: database connection
    :param tnsNames: list of dicts of tns names and internal names for association
    :return reports: the list of TNS report IDs processed

    """
    import MySQLdb

    updatedRows = 0
    reports = []

    try:
        cursor = conn.cursor (MySQLdb.cursors.DictCursor)

        for name in tnsNames:
            if name['exists']:
                logger.info('The name %s (%s) already exists on the nameserver.' % (name['objname'], name['internal_name']))

            tnsName = name['objname']
            if tnsName[0:2] == 'AT' or tnsName[0:2] == 'SN':
                # Strip the prefix
                tnsName = tnsName[2:]

            # TEMPORARY FIX before I add the relevant index.
            # If the query below doesn't use detection_list_id
            # it will take forever.  There is currently no index
            # on the atlas_diff_objects atlas_designation or other_designation
            # columns.

            # For the time being ONLY update the other_designation if the object is NEW.
            if not name['exists']:
                cursor.execute ("""
                    update atlas_diff_objects
                    set other_designation = %s
                    where atlas_designation = %s
                    and detection_list_id > 0
                    """, (tnsName, name['internal_name']))

            reports.append(name['report_id'])

        updatedRows = cursor.rowcount
        cursor.close()

    except MySQLdb.Error as e:
        sys.stderr.write("Error %d: %s\n" % (e.args[0], e.args[1]))
        reports = []

    return reports


def getLastNonDetection(conn, candidate, sdssRadius = 300, sdssCatalogue = 'tcs_cat_sdss_dr9_photo_stars_galaxies', ddc = False, discoveryMJD = None):
    """
    Get the last non-detection info if it exists.  If not check if the object is in the SDSS
    footprint and set the non-detection archiveid to SDSS, otherwise set it to DSS.

    :param conn: database connection
    :param candidate: 
    :param sdssRadius:  (Default value = 300)
    :param sdssCatalogue:  (Default value = 'tcs_cat_sdss_dr9_photo_stars_galaxies')
    :return nonDetectionData: dict containing non-detection data

    """
    from commonqueries import getLightcurvePoints, getNonDetections, getNonDetectionsUsingATLASFootprint, ATLAS_METADATADDC, LC_POINTS_QUERY_ATLAS_DDC, filterWhereClauseddc, FILTERS
    from utils import coneSearchHTM

    archiveData = None

    if ddc:
        p, recurrences = getLightcurvePoints(candidate['id'], lcQuery=LC_POINTS_QUERY_ATLAS_DDC + filterWhereClauseddc(FILTERS), conn = conn)
        b, blanks, lastNonDetection = getNonDetectionsUsingATLASFootprint(recurrences, conn = conn, ndQuery=ATLAS_METADATADDC, filterWhereClause = filterWhereClauseddc, catalogueName = 'atlas_metadataddc')
    else:
        p, recurrences = getLightcurvePoints(candidate['id'], conn = conn)
        b, blanks, lastNonDetection = getNonDetections(recurrences, conn = conn, tolerance = 0.001)

    # 2017-10-22 KWS New code to make sure that the last non-detection != detection.
    #                We did this in PS1, but not for ATLAS!
    #mjdTolerance = 0.001
    # 2018-05-21 KWS Don't use a last non-detection from within the same day as the detection
    #                because this is extremely rare anyway. Go back at least half a day.
    # 2018-05-21 KWS We would like to get the day the object was triggered. And preferably
    #                the epoch BEFORE the object was triggered. We could do this by modifying
    #                the apply cuts code to return those values. If an object was promoted
    #                manually, then revert back to previous method of determining trigger date.
    mjdTolerance = 0.5

    detectionBeforeTrigger = None
    if discoveryMJD:
        # 2018-05-21 KWS We want to set the discovery date as the first detection on the
        #                date that we had the right number of detections, etc.
        for row in reversed(recurrences):
            if row.mjd < discoveryMJD - mjdTolerance:
                detectionBeforeTrigger = row
                break

        lastNonDetection = None # Ignore what the non-det code gave you... do it from scratch
        for row in reversed(blanks):
            if row.mjd < discoveryMJD - mjdTolerance:
                # This gets set on each iteration, but if mjd >= first detection mjd then
                # this value should contain the last non-detection epoch (or nothing). 
                lastNonDetection = row
                break

    else:
        # Carry on as before...
        if lastNonDetection and lastNonDetection.mjd >= recurrences[0].mjd - mjdTolerance:
            # Iterate through the blanks to find a non-detection that
            # meets the tolerance criterion. Otherwise leave blank.
            lastNonDetection = None
            for row in reversed(blanks):
                if row.mjd < recurrences[0].mjd - mjdTolerance:
                    # This gets set on each iteration, but if mjd >= first detection mjd then
                    # this value should contain the last non-detection epoch (or nothing). 
                    lastNonDetection = row
                    break

    if detectionBeforeTrigger and lastNonDetection:
        # Which one is latest?
        if detectionBeforeTrigger.mjd > lastNonDetection.mjd:
            lastNonDetection = detectionBeforeTrigger

    nonDetectionData = {}

    if lastNonDetection:
        # Add the last non-detection dict.
        lastDetectionDate = getDateFractionMJD(lastNonDetection.mjd, delimiter = '-', decimalPlaces = 5)
        nonDetectionData = { 'obsdate': getDateFractionMJD(lastNonDetection.mjd, delimiter = '-', decimalPlaces = 5),
                             'limiting_flux': str(lastNonDetection.mag5sig),
                             'flux_units': '1',
                             'filter_value': ATLASFilters[lastNonDetection.filter],
                             'instrument_value': ATLASInstrument[lastNonDetection.expname[0:3]],
                             'exptime': str(lastNonDetection.exptime),
                             'observer': 'Robot'
                            }

    else:
        # Check for SDSS object.
        message, results = coneSearchHTM(candidate['ra'], candidate['dec'], sdssRadius, sdssCatalogue, queryType = QUICK, conn = conn)
        if results:
            # We don't need the results, only that there were some.  We'll log how
            # many results we got, but that more than we need.
            logger.info("We got %d SDSS results" % len(results))
            archiveData = TNS_ARCHIVE['SDSS']
        else:
            archiveData = TNS_ARCHIVE['DSS']

        nonDetectionData = { 'archiveid': archiveData }

    return nonDetectionData


# 2018-10-12 KWS Now pass TNS authors to the function. Allows me to read them from the config file.
def tnsReport(conn, tnsBaseURL, tnsApiKey, objectList, ddc = False, donotsend = False, reporter = TNSAUTHORS, tnsBaseURLExperimental = None, tnsApiKeyExperimental = None, botId = None, botName = None, addInternalIDs = False):
    """
    Construct and send reports to the TNS, with a maximum of 100 objects
    at a time.

    :param conn: database connection
    :param tnsBaseURL: TNS base URL
    :param tnsApiKey: TNS API Key
    :param objectList: 
    :return reports: the report IDs sent to the Transient Nameserver

    """

    reports = []
    groupId = ATLASGroup
    #reporter = "J. Tonry, L. Denneau, B. Stalder, A. Heinze, A. Sherstyuk (IfA, University of Hawaii), A. Rest (STScI), K. W. Smith, S. J. Smartt (Queen's University Belfast)"
    # 2017-01-19 KWS Updated author list.
    # 2018-10-12 KWS Now pass the TNS Authors into the function. This allows us to place the authors into the config file.
    #reporter = TNSAUTHORS
    #reporter = "K. W. Smith"
    atType = 1
    instrument = ATLASInstrument['02a']
    
    arrayLength = len(objectList)
    maxNumberOfCandidates = 100
    numberOfIterations = arrayLength/maxNumberOfCandidates

    # Check to see if we need an extra iteration to clean up the end of the array
    if arrayLength%maxNumberOfCandidates != 0:
        numberOfIterations += 1

    logger.info("Number of iterations = %d" % numberOfIterations)

    for currentIteration in range(numberOfIterations):
        candidateArray = objectList[currentIteration*maxNumberOfCandidates:currentIteration*maxNumberOfCandidates+maxNumberOfCandidates]
        logger.info("Iteration %d" % (currentIteration + 1))
        tnsDict = {'at_report': {} }
        counter = 0
        for row in candidateArray:
            triggerMJD = None
            discoveryMJD = None
            discoveryMag = None
            discoveryFilter = None
            discoveryInstrument = None
            discoveryExptime = None
            limitingMag = None
            if ddc:
                lc = getObjectInfo(conn, row['id'])
                testResults = testObject(conn, row['id'], mjdWindow = 100, debug = False, followupFlagDate = row['followup_flag_date'].strftime("%Y-%m-%d"))
                triggerMJD = testResults['triggerMJD']
                print(triggerMJD)
                for recurrence in lc:
                    if recurrence['MJD'] > triggerMJD:
                        discoveryMJD = recurrence['MJD']
                        discoveryMag = recurrence['mag']
                        discoveryMagError = recurrence['dm']
                        discoveryFilter = ATLASFilters[recurrence['Filter']]
                        discoveryInstrument = ATLASInstrument[recurrence['expname'][0:3]]
                        discoveryExptime = recurrence['exptime']
                        limitingMag = recurrence['mag5sig']
                        break
                # If we didn't find our discoveryMJD, work it out the old way
                if discoveryMJD is None:
                    discoveryMJD = lc[0]['MJD']
                    discoveryMag = lc[0]['mag']
                    discoveryMagError = lc[0]['dm']
                    discoveryFilter = ATLASFilters[lc[0]['Filter']]
                    discoveryInstrument = ATLASInstrument[lc[0]['expname'][0:3]]
                    discoveryExptime = lc[0]['exptime']
                    limitingMag = lc[0]['mag5sig']
            else:
                lc = getObjectInfo(conn, row['id'])
                discoveryMJD = lc[0]['MJD']
                discoveryMag = lc[0]['mag']
                discoveryMagError = lc[0]['dm']
                discoveryFilter = ATLASFilters[lc[0]['Filter']]
                discoveryInstrument = ATLASInstrument[lc[0]['expname'][0:3]]
                discoveryExptime = lc[0]['exptime']
                limitingMag = lc[0]['mag5sig']
            # calculate average RA and Dec
            # raAvg, decAvg, rms = calculateRMSScatter(lc)
            raAvg = row['ra']
            decAvg = row['dec']
            discoveryDate = getDateFractionMJD(discoveryMJD, delimiter = '-', decimalPlaces = 5)
            internalName = row['atlas_designation']

            nonDetectionData = getLastNonDetection(conn, row, ddc = ddc, discoveryMJD = discoveryMJD)

            # 2020-01-15 KWS changed 'groupid': str(groupId) to 'reporting_group_id': str(groupId) and 'discovery_data_source_id': str(groupId)
            #                as per instructions from Ofer Yaron and Avner Sass on 2019-11-24.
            tnsDict['at_report'][str(counter)] = {'ra': {'value': str(raAvg)},
                                                  'dec': {'value': str(decAvg)},
                                                  'internal_name': internalName,
                                                  'discovery_datetime': discoveryDate,
                                                  'at_type': str(atType),
                                                  'reporting_group_id': str(groupId),
                                                  'discovery_data_source_id': str(groupId),
                                                  'reporter': reporter,
                                                  'photometry': {'photometry_group': {str(counter): {'obsdate': discoveryDate,
                                                                                                     'flux': str(discoveryMag),
                                                                                                     'flux_error': str(discoveryMagError),
                                                                                                     'limiting_flux': str(limitingMag),
                                                                                                     'flux_units': '1',
                                                                                                     'filter_value': discoveryFilter,
                                                                                                     'instrument_value': discoveryInstrument,
                                                                                                     'exptime': str(discoveryExptime),
                                                                                                     'observer': 'Robot',
                                                                                                     'comments': ''}
                                                                                       }
                                                                 },
                                                  'non_detection': nonDetectionData,
                                                  'proprietary_period_groups': [ str(groupId) ],
                                                  'proprietary_period': { 'proprietary_period_value': '0',
                                                                          'proprietary_period_units': 'days' },
                                                  }

            if addInternalIDs:
                tnsDict['at_report'][str(counter)]['internal_ids'] = {"internal_name": internalName,
                                                                      "internal_objid": str(row['id'])}


            counter += 1

        # We have now constructed the TNS dictionary. Send it to the TNS.  Record the report id in the database.
        logger.debug("REQUEST")
        logger.debug(json.dumps(tnsDict, indent=4, sort_keys=True))
 
        if not donotsend:
            reportId = addBulkReport(tnsDict, tnsBaseURL, tnsApiKey, botId = botId, botName = botName)
            if reportId:
                tnsAddRequestToDatabase(conn, reportId)
                updateTNSRequestFlag(conn, objectList)
                reports.append(reportId)
            #if tnsBaseURLExperimental is not None and tnsApiKeyExperimental is not None:
            #    print "Sending Experimental request"
            #    newReportId = addBulkReport(tnsDict, tnsBaseURLExperimental, tnsApiKeyExperimental)
            #    if newReportId:
            #        logger.debug("Experimental request submitted. ReportId = %s" % newReportId)
            #    else:
            #        print "Report ID was null"

        # Sleep for at least 1 second before sending the next report
        time.sleep(1)

    return reports


def main(argv=None):
    opts = docopt(__doc__, version='0.1')
    opts = cleanOptions(opts)
    options = Struct(**opts)


    import yaml
    with open(options.configfile) as yaml_file:
        config = yaml.safe_load(yaml_file)

    candidateList = []

    import yaml
    with open(options.configfile) as yaml_file:
        config = yaml.safe_load(yaml_file)

    username = config['databases']['local']['username']
    password = config['databases']['local']['password']
    database = config['databases']['local']['database']
    hostname = config['databases']['local']['hostname']

    conn = dbConnect(hostname, username, password, database)
    # 2023-03-13 KWS Need to set autocommit for all transactions with InnoDB tables. Set to False by default.
    conn.autocommit(True)

    update = options.update

    detectionList = None
    customList = None
    objectList = []

    tnsBaseURLExperimental = None
    tnsApiKeyExperimental = None
    tnsAuthors = config['tns_api']['atlas']['authors']
    if options.live:
        tnsBaseURL = config['tns_api']['atlas']['live']['baseurl']
        tnsApiKey = config['tns_api']['atlas']['live']['api_key']
        tnsBaseURLExperimental = config['tns_api']['atlas']['experimental']['baseurl']
        tnsApiKeyExperimental = config['tns_api']['atlas']['experimental']['api_key']
        botId = config['tns_api']['atlas']['live']['bot_id']
        botName = config['tns_api']['atlas']['live']['bot_name']
    else:
        tnsBaseURL = config['tns_api']['atlas']['sandbox']['baseurl']
        tnsApiKey = config['tns_api']['atlas']['sandbox']['api_key']
        botId = config['tns_api']['atlas']['sandbox']['bot_id']
        botName = config['tns_api']['atlas']['sandbox']['bot_name']

    if options.getreports:
        names = getSubmissionReports(conn, tnsBaseURL, tnsApiKey)
        if not names:
            sys.stderr.write("Bad response. Looks like the report does not exist yet\n")
        else:
            reports = updateTNSNames(conn, names)
            if reports:
                # Set the relevant request ID to be completed
                for reportId in set(reports):
                    tnsUpdateRequestStatus(conn, reportId, COMPLETE)

    else:
        # 2018-04-27 KWS Override the object list and feed specific objects
        if len(args) > 1:
            for i in range(1,len(args)):
                o = getObjectById(conn, objectId = int(args[i]))
                if o:
                    objectList.append(o)
        elif options.customlist is not None:
            if options.customlist > 0 and options.detectionlist < 100:
                customList = options.customlist
                objectList = getObjectsByCustomList(conn, customList)
            else:
                sys.stderr.write("The list must be between 1 and 100 inclusive.  Exiting.")
                sys.exit(1)
        else:
            if options.detectionlist is not None:
                if options.detectionlist >= 0 and options.detectionlist < 7:
                    detectionList = options.detectionlist
                    objectList = getObjectsByList(conn, detectionList)
                else:
                    sys.stderr.write("The list must be between 0 and 6 inclusive.  Exiting.")
                    sys.exit(1)

        reports = tnsReport(conn, tnsBaseURL, tnsApiKey, objectList, ddc = options.ddc, donotsend = options.donotsend, reporter = tnsAuthors, tnsBaseURLExperimental = tnsBaseURLExperimental, tnsApiKeyExperimental = tnsApiKeyExperimental, botId = botId, botName = botName, addInternalIDs = options.internalids) 
        for row in reports:
            print("TNS report ID = %s" % row)

    return 0


if __name__ == '__main__':
    main()
