#!/usr/bin/env python
"""Get ATLAS TNS names.

Usage:
  %s <configfile> [<objectid>...] [--list=<list>] [--customlist=<customlist>] [--getreports] [--live] [--internalids] [--donotsend] [--skipnondetections] [--flagdate=<flagdate>] [--zoousers=<zoousers>] [--proprietaryperiod=<proprietaryperiod>] [--proprietaryunits=<proprietaryunits>] [--kegs]
  %s (-h | --help)
  %s --version

Options:
  -h --help                                 Show this screen.
  --version                                 Show version.
  --list=<list>                             Grab objects from the specified list.
  --customlist=<customlist>                 Grab objects from a custom list.
  --update                                  Update the database
  --getreports                              Get reports from the TNS
  --live                                    Send to the LIVE TNS. By default reports go to the Sandbox.
  --internalids                             Add the new internal_ids key as custom key/value pair dictionary
  --donotsend                               Do not send a report to TNS. Just test.
  --skipnondetections                       Do not send non-detections. Just include SDSS or DSS archive comment.
  --flagdate=<flagdate>                     Date threshold - no hyphens [default: 20160724]
  --zoousers=<zoousers>                     Comma separated list of Supernova Hunter usernames with no spaces [default: ]
  --proprietaryperiod=<proprietaryperiod>   Proprietary period [default: 0]
  --proprietaryunits=<proprietaryunits>     Proprietary units (days, months or years). [default: days]
  --kegs                                    Add the KEGS authors

Example:

"""

import sys
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt
import json
import time
import logging

from tnsUtils import tnsAddRequestToDatabase, tnsUpdateRequestStatus, tnsUpdateRequestDownloadAttempts, tnsGetRequestList, getSubmissionReports

from gkutils.commonutils import dbConnect, PROCESSING_FLAGS, calculateRMSScatter, getDateFractionMJD, coneSearchHTM, QUICK, grammarJoin, FLAGS, getMJDFromSqlDate
#from apply3piDiffCuts import getObjectInfo
from math import log10
from collections import OrderedDict


# Use the TNS logger /tmp/tns.log to record log info
logger = logging.getLogger(__name__)

# 2022-02-05 KWS Updated the griz filters to be the newly defined g-P1, r-P1, i-P1, z-P1 on TNS.
FILTERS = {'g': '56', 'r': '57', 'i': '58', 'z': '59', 'y': '25', 'w': '26'}
INSTRUMENT = {'GPC1': '155', 'GPC2': '257'}
GROUP = 4

# 2017-06-20 KWS Placed authors at top of this file and added KEGS team. KEGS
#                team authors will be added automatically if FLAGS['kepler'] is set.
# 2017-08-19 KWS Added new PS1 observer.
#PANSTARRSTEAM = "K. C. Chambers, M. E. Huber, H. Flewelling, E. A. Magnier, A. Schultz, T. Lowe, J. Bulger (IfA, University of Hawaii), S. J. Smartt, K. W. Smith (Queen's University Belfast), J. Tonry, C. Waters, (IfA, University of Hawaii) D. E. Wright (University of Minnesota), D. R. Young (Queen's University Belfast)"

PANSTARRSTEAM = "K. C. Chambers, T. de Boer, J. Bulger, J. Fairlamb, M. Huber, C.-C. Lin, T. Lowe, E. Magnier, A. Schultz, R. J. Wainscoat, M. Willman (IfA, University of Hawaii), K. W. Smith, D. R. Young, O. McBrien, J. Gillanders. S. Srivastav, S. J. Smartt, D. O'Neil, P. Clark, S. Sim (Queen's University Belfast), D. E. Wright (University of Minnesota)"

KEGSTEAM = "A. Rest (STScI), B. E. Tucker (Mt Stromlo Observatory, ANU), G. Narayan (STScI), P. M. Garnavich (Notre Dame), S. Margheim (Gemini Observatory), D. Kasen (UCB/LBL), R. Olling, E. Shaya (University of Maryland)"
PANSTARRSTEAM_KEGSTEAM = PANSTARRSTEAM + ", " + KEGSTEAM

LC_LIMITS_3PI = {"g": 21.0,
                 "r": 20.6,
                 "i": 20.7,
                 "z": 20.4,
                 "y": 18.3,
                 "w": 22.0,
                 "x": 19.5}


# A selection of possible responses.  We currently only use 1 and 2.
SUBMITTED           = 1
COMPLETE            = 2
COMMUNICATION_ERROR = 3
TIMEOUT             = 4
DOWNLOADING         = 5
CORRUPT             = 7


ZOO_STRING = 'This transient was discovered during the citizen science project Supernova Hunters run by the Zooniverse. The candidate was identified by volunteers including'

def getObjectsByList(conn, listId = 4, dateThreshold = '2020-01-01'):
    """
    Get the PS1 objects we want to register by object list

    :param conn: database connection
    :param listId:  (Default value = 4)
    :param dateThreshold:  (Default value = '2019-01-01')
    :return resultSet: The tuple of object dicts

    """
    import MySQLdb

    try:
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)

        cursor.execute ("""
            select id, if(ra_psf < 0, ra_psf + 360.0, ra_psf) ra, dec_psf `dec`, id 'name', followup_flag_date, ps1_designation, object_classification
            from tcs_transient_objects
            where detection_list_id = %s
            and (processing_flags & %s = 0 or processing_flags is null)
            and followup_flag_date > %s
            and ps1_designation is not null
            and observation_status != 'mover'
            order by followup_id
        """, (listId, PROCESSING_FLAGS['tns'], dateThreshold))

        resultSet = cursor.fetchall ()
        cursor.close ()

    except MySQLdb.Error as e:
        sys.stderr.write("Error %d: %s\n" % (e.args[0], e.args[1]))
        sys.exit (1)

    return resultSet


def getObjectsByCustomList(conn, customList):
    """

    :param conn: database connection
    :param customList: 
    :return resultSet: The tuple of object dicts

    """
    import MySQLdb

    try:
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute ("""
            select o.id, if(ra_psf < 0, ra_psf + 360.0, ra_psf) ra, o.dec_psf `dec`, o.id 'name', o.followup_flag_date, o.ps1_designation, o.object_classification
            from tcs_transient_objects o, tcs_object_groups g
            where g.object_group_id = %s
              and g.transient_object_id = o.id
              and (o.processing_flags & %s = 0 or o.processing_flags is null)
              and o.ps1_designation is not null
              and o.observation_status != 'mover'
            order by o.followup_id
        """, (customList, PROCESSING_FLAGS['tns']))

        resultSet = cursor.fetchall ()
        cursor.close ()

    except MySQLdb.Error as e:
        sys.stderr.write("Error %d: %s\n" % (e.args[0], e.args[1]))
        sys.exit (1)

    return resultSet

# 2019-08-07 KWS Altered the code to always produce the MEAN coordinates.
def getSpecifiedObjects(conn, objectIds):
    """

    :param conn: database connection
    :param objectIds: 
    :return resultSet: The tuple of object dicts

    """
    import MySQLdb

    try:
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)
        objectList = []
        for id in objectIds:
            cursor.execute ("""
                select o.id, o.local_designation, a.ra_avg ra, a.dec_avg `dec`, o.id 'name', o.followup_flag_date, o.ps1_designation, o.object_classification from
                (
                   select id, avg(ra_psf) ra_avg, avg(dec_psf) dec_avg
                   from
                   (
                       select id, if(ra_psf<0, ra_psf + 360.0, ra_psf) ra_psf, dec_psf
                       from tcs_transient_objects
                       where id = %s
                       union all
                       select transient_object_id id, if(ra_psf<0, ra_psf + 360.0, ra_psf) ra_psf, dec_psf
                       from tcs_transient_reobservations
                       where transient_object_id = %s
                   ) temp group by id
                ) a, tcs_transient_objects o
                where a.id = o.id
                and o.id = %s
                and o.observation_status != 'mover'
                and (o.processing_flags & %s = 0 or o.processing_flags is null)
                and o.ps1_designation is not null
            """, (id, id, id, PROCESSING_FLAGS['tns']))

            resultSet = cursor.fetchone ()

            if resultSet is not None and len(resultSet) > 0:
                objectList.append(resultSet)

        cursor.close ()

    except MySQLdb.Error as e:
        sys.stderr.write("Error %d: %s\n" % (e.args[0], e.args[1]))
        sys.exit (1)

    return objectList


# 2020-04-29 KWS Copied local version of getObjectInfo so we can make sure that we ONLY
#                report objects that have non NULL cal_psf_mag and psf_inst_mag_sig.
def getObjectInfo(conn, objectId):
   """
   Get all object occurrences. Grab the quality data as well so we can make a subsequent
   decision to reject the recurrence if necessary.
   """
   import MySQLdb

   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      # Note that DEC is a MySQL reserved word, so need quotes around it

      # 2011-10-12 KWS Addded imageid to order by clause to make sure that the results
      #                are ordered consistently
      # 2013-08-26 KWS Addded flags so we can check for ghosts
      cursor.execute ("""
            SELECT d.ra_psf RA,
                   d.dec_psf 'DEC',
                   m.imageid,
                   substr(m.fpa_filter,1,1) Filter,
                   m.mjd_obs MJD,
                   m.filename Filename,
                   d.cal_psf_mag,
                   d.psf_inst_mag,
                   d.psf_inst_mag_sig,
                   d.ap_mag,
                   d.moments_xy,
                   d.flags,
                   m.zero_pt,
                   m.exptime,
                   m.fpa_comment,
                   m.fpa_obs_mode,
                   m.fpa_detector
            FROM tcs_transient_objects d, tcs_cmf_metadata m
            where d.id=%s
            and d.tcs_cmf_metadata_id = m.id
            and (m.filename like '%%WS%%' or m.filename like '%%SS%%')
            and (d.deprecated is null or d.deprecated != 1)
            and d.cal_psf_mag is not null
            and d.psf_inst_mag_sig is not null
            UNION ALL
            SELECT d.ra_psf RA,
                   d.dec_psf 'DEC',
                   m.imageid,
                   substr(m.fpa_filter,1,1) Filter,
                   m.mjd_obs MJD,
                   m.filename Filename,
                   d.cal_psf_mag,
                   d.psf_inst_mag,
                   d.psf_inst_mag_sig,
                   d.ap_mag,
                   d.moments_xy,
                   d.flags,
                   m.zero_pt,
                   m.exptime,
                   m.fpa_comment,
                   m.fpa_obs_mode,
                   m.fpa_detector
            FROM tcs_transient_reobservations d, tcs_cmf_metadata m
            where d.transient_object_id=%s
            and d.tcs_cmf_metadata_id = m.id
            and (m.filename like '%%WS%%' or m.filename like '%%SS%%')
            and (d.deprecated is null or d.deprecated != 1)
            and d.cal_psf_mag is not null
            and d.psf_inst_mag_sig is not null
            ORDER by MJD, imageid
      """, (objectId, objectId))
      result_set = cursor.fetchall ()

      cursor.close ()

   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))
      sys.exit (1)


   return result_set

# 2020-02-13 KWS Get any zooniverse information that is related to this object.
def getZooniverseInformation(conn, candidate):
    """

    :param conn: database connection
    :param candidate: 
    :return resultSet: a result dictionary

    """
    import MySQLdb

    try:
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute ("""
            select *
              from tcs_zooniverse_scores
             where transient_object_id = %s
        """, (candidate,))

        resultSet = cursor.fetchone ()

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
                update tcs_transient_objects
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
            # on the atlas_diff_objects ps1_designation or other_designation
            # columns.

            # For the time being ONLY update the other_designation if the object is NEW.
            if not name['exists']:
                cursor.execute ("""
                    update tcs_transient_objects
                    set other_designation = %s
                    where ps1_designation = %s
                    and detection_list_id > 0
                    """, (tnsName, name['internal_name']))

            reports.append(name['report_id'])

        updatedRows = cursor.rowcount
        cursor.close()

    except MySQLdb.Error as e:
        sys.stderr.write("Error %d: %s\n" % (e.args[0], e.args[1]))
        reports = []

    return reports


# Group lightcurves into whole MJDs.
def groupLCIntoMJDs(lc):
    mjds = OrderedDict()

    for l in lc:
        try:
            mjds[int(l['MJD'])].append(l)
        except KeyError as e:
            mjds[int(l['MJD'])] = []
            mjds[int(l['MJD'])].append(l)

    return mjds


def getLastNonDetection(conn, candidate, sdssRadius = 300, sdssCatalogue = 'tcs_cat_sdss_dr9_photo_stars_galaxies', skipNonDetections = False):
    """
    Get the last non-detection info if it exists.  If not check if the object is in the SDSS
    footprint and set the non-detection archiveid to SDSS, otherwise set it to DSS.

    :param conn: database connection
    :param candidate: 
    :param sdssRadius:  (Default value = 300)
    :param sdssCatalogue:  (Default value = 'tcs_cat_sdss_dr9_photo_stars_galaxies')
    :return nonDetectionData: dict containing non-detection data

    """
    from utils import coneSearchHTM
    from pstampRequestStamps import getLightcurveNonDetectionsAndBlanks

    archiveData = None

    recurrences = getObjectInfo(conn, candidate['id'])
    blanks = getLightcurveNonDetectionsAndBlanks(conn, candidate['id'])
    lastNonDetection = None

    tolerance = 0.001
    for row in blanks:
        if row['mjd'] < recurrences[0]['MJD'] - tolerance:
            # This gets set on each iteration, but if mjd >= first detection mjd then
            # this value should contain the last non-detection epoch (or nothing). 
            lastNonDetection = row

    # The "blank" immediately before the first detection MJD is the one we want.

    # Note for the future - if we have an image associated with this recurrence
    # we should check for NaNs at the centre of this image. If there are NaNs,
    # step back until we find a detection with no NaNs (i.e. not in a chip gap).
    # If we run out of detections with images, use the non-detection before the
    # first non-detection image. Otherwise just revert to Sloan or DSS.

    nonDetectionData = {}

    if lastNonDetection and not skipNonDetections:
        # Add the last non-detection dict.
        lastDetectionDate = getDateFractionMJD(lastNonDetection['mjd'], delimiter = '-', decimalPlaces = 5)
        try:
            ndLimitingMag = lastNonDetection['deteff_magref'] + 2.5 * log10(lastNonDetection['exptime']) + lastNonDetection['zero_pt'] if lastNonDetection['deteff_counts'] < 400 else lastNonDetection['deteff_magref'] + 2.5 * log10(lastNonDetection['exptime']) + lastNonDetection['zero_pt'] + lastNonDetection['deteff_calculated_offset']
        except KeyError as e:
            # There is no limiting mag column yet
            ndLimitingMag = LC_LIMITS_3PI[lastNonDetection['filter']]

        nonDetectionData = { 'obsdate': getDateFractionMJD(lastNonDetection['mjd'], delimiter = '-', decimalPlaces = 5),
                             'limiting_flux': "%.02f" % ndLimitingMag,
                             'flux_units': '1',
                             'filter_value': FILTERS[lastNonDetection['filter']],
                             'instrument_value': INSTRUMENT[lastNonDetection['fpa_detector']],
                             'exptime': str(lastNonDetection['exptime']),
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


def tnsReport(conn, tnsBaseURL, tnsApiKey, objectList, skipNonDetections = False, zooniverseUsers = '', proprietaryPeriod = 0, proprietaryUnits = 'days', kegs = False, reporter = PANSTARRSTEAM, supplementaryAuthors = '', supplementaryAuthorsTrigger = '', reportingGroupId = GROUP, discoveryDataSourceId = GROUP, donotsend = False, zooniverseBoilerplate = '', zooniverseScoreThreshold = 0.95, botId = None, botName = None, addInternalIDs = False):
    """
    Construct and send reports to the TNS, with a maximum of 20 objects
    at a time.

    :param conn: database connection
    :param tnsBaseURL: TNS base URL
    :param tnsApiKey: TNS API Key
    :param objectList: 
    :return reports: the report IDs sent to the Transient Nameserver

    """

    reports = []
    groupId = GROUP
    #reporter = "K. C. Chambers, M. E. Huber, H. Flewelling, E. A. Magnier, A. Schultz, T. Lowe (IfA, University of Hawaii), S. J. Smartt, K. W. Smith, (Queen's University Belfast), J. Tonry, C. Waters (IfA, University of Hawaii), D. E. Wright (University of Minnesota), D. R. Young (Queen's University Belfast)"
    #reporter = PANSTARRSTEAM
    #reporter = "K. W. Smith"
    atType = 1
    instrument = INSTRUMENT['GPC1']
    
    arrayLength = len(objectList)
    maxNumberOfCandidates = 20
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

            # 2017-06-20 KWS Was this discovered as a result of Kepler K2 campaign?
            if (row['object_classification'] & FLAGS['kepler'] == FLAGS['kepler']) or kegs:
                reporter = PANSTARRSTEAM_KEGSTEAM
            else:
                reporter = reporter

            zooniverseInfo = getZooniverseInformation(conn, row['id'])
            zooniverseUsers = []
            zooniverseUserText = ''
            if zooniverseInfo:
                if zooniverseInfo['score'] >= zooniverseScoreThreshold:
                    zooniverseUserText = zooniverseBoilerplate
                    if zooniverseInfo['user1'] and zooniverseInfo['user1'].strip() and zooniverseInfo['user1'] != 'NULL' and zooniverseInfo['user1'] != 'None':
                        zooniverseUsers.append(zooniverseInfo['user1'])
                    if zooniverseInfo['user2'] and zooniverseInfo['user2'].strip() and zooniverseInfo['user2'] != 'NULL' and zooniverseInfo['user2'] != 'None':
                        zooniverseUsers.append(zooniverseInfo['user2'])
                    if zooniverseInfo['user3'] and zooniverseInfo['user3'].strip() and zooniverseInfo['user3'] != 'NULL' and zooniverseInfo['user3'] != 'None':
                        zooniverseUsers.append(zooniverseInfo['user3'])
                    if len(zooniverseUsers) > 0:
                        zooniverseUserText += ' The candidate was identified by volunteers including ' + grammarJoin((list(set(zooniverseUsers))))

            lc = getObjectInfo(conn, row['id'])
            # calculate average RA and Dec
            raAvg, decAvg, rms = calculateRMSScatter(lc)
            # 2016-08-09 KWS Is the RA negative??  This can happen with PS1 objects.
            if raAvg < 0.0:
                raAvg = raAvg + 360.0

            # 2022-09-01 KWS Use the flag date MJD to walk back to the first detection just before the flag date.
            #                Do NOT use the zeroth lightcurve point if it can be avoided.

            triggerRow = None

            followupFlagMJD = getMJDFromSqlDate(row['followup_flag_date'].strftime("%Y-%m-%d") + ' 00:00:00') + 1
            # March backwards now to the first detection on the first MJD before the flag date.

            mjds = groupLCIntoMJDs(reversed(lc))

            for m, lcrow in mjds.items():
                if m < followupFlagMJD:
                    # Stop - our condition is satisfied.
                    triggerRow = sorted(lcrow, key=lambda x: (x['MJD']))[0]
                    break

            if triggerRow is None:
                # We didn't find the correct trigger row. So do it the old way!
                triggerRow = lc[0]

            discoveryDate = getDateFractionMJD(triggerRow['MJD'], delimiter = '-', decimalPlaces = 5)

            discoveryMag = triggerRow['cal_psf_mag']
            discoveryMagError = triggerRow['psf_inst_mag_sig']
            discoveryFilter = FILTERS[triggerRow['Filter']]
            discoveryInstrument = INSTRUMENT[triggerRow['fpa_detector']]
            discoveryExptime = triggerRow['exptime']

            # 2019-09-02 KWS Was this discovered as a result of NCU data?
            try:
                if supplementaryAuthorsTrigger and supplementaryAuthorsTrigger in triggerRow['fpa_obs_mode'] and supplementaryAuthors:
                    reporter = reporter + ', ' + supplementaryAuthors
            except TypeError as e:
                # Don't care if we get None back for fpa_obs_mode. None is not iterable
                # so we will get a TypeError if this happens.
                pass

            try:
                limitingMag = triggerRow['deteff_magref'] + 2.5 * log10(triggerRow['exptime']) + triggerRow['zero_pt'] if triggerRow['deteff_counts'] < 400 else triggerRow['deteff_magref'] + 2.5 * log10(triggerRow['exptime']) + triggerRow['zero_pt'] + triggerRow['deteff_calculated_offset']
            except KeyError as e:
                # There is no limiting mag column yet
                limitingMag = LC_LIMITS_3PI[triggerRow['Filter']]

            if limitingMag is None:
                limitingMag = LC_LIMITS_3PI[triggerRow['Filter']]

            internalName = row['ps1_designation']

            nonDetectionData = getLastNonDetection(conn, row, skipNonDetections = skipNonDetections)

            # 2020-01-15 KWS changed 'groupid': str(groupId) to 'reporting_group_id': str(groupId) and 'discovery_data_source_id': str(groupId)
            #                as per instructions from Ofer and Avner on 2019-11-24.
            tnsDict['at_report'][str(counter)] = {'ra': {'value': str(raAvg)},
                                                  'dec': {'value': str(decAvg)},
                                                  'internal_name': internalName,
                                                  'discovery_datetime': discoveryDate,
                                                  'at_type': str(atType),
                                                  'reporting_group_id': str(reportingGroupId),
                                                  'discovery_data_source_id': str(discoveryDataSourceId),
                                                  'reporter': reporter,
                                                  'photometry': {'photometry_group': {str(counter): {'obsdate': discoveryDate,
                                                                                                     'flux': "%.02f" % discoveryMag,
                                                                                                     'flux_error': "%.02f" % discoveryMagError,
                                                                                                     'limiting_flux': "%.02f" % limitingMag,
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
                                                  'proprietary_period': { 'proprietary_period_value': str(proprietaryPeriod),
                                                                          'proprietary_period_units': proprietaryUnits },
                                                  }

            if addInternalIDs:
                tnsDict['at_report'][str(counter)]['internal_ids'] = {"internal_name": internalName,
                                                                      "internal_objid": str(row['id'])}

            #if zooniverseUsers:
            #    tnsDict['at_report'][str(counter)]['remarks'] = ZOO_STRING + ' ' + zooniverseUsers + '.'
            if zooniverseUserText:
                tnsDict['at_report'][str(counter)]['remarks'] = zooniverseUserText

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

    username = config['databases']['local']['username']
    password = config['databases']['local']['password']
    database = config['databases']['local']['database']
    hostname = config['databases']['local']['hostname']

    conn = dbConnect(hostname, username, password, database)
    conn.autocommit(True)

    update = options.update
    proprietaryUnits = options.proprietaryunits
    if proprietaryUnits not in ['days', 'months', 'years']:
        # Just default to days
        proprietaryUnits = 'days'

    flagDate = '2016-07-24'
    # Date on which this PS1 API switched on.
    if options.flagdate is not None:
        try:
            flagDate = '%s-%s-%s' % (options.flagdate[0:4], options.flagdate[4:6], options.flagdate[6:8])
        except:
            flagDate = '2016-07-24'

    zooniverseUsers = ""
    #if options.zoousers:
    #    try:
    #        zooniverseUsers = grammarJoin(["'%s'" % x for x in options.zoousers.split(',')])
    #    except:
    #        zooniverseUsers = ""

    detectionList = None
    customList = None

    tnsAuthors = config['tns_api']['ps1']['authors']

    supplementaryAuthors = ''
    try:
        supplementaryAuthors = config['tns_api']['ps1']['supplementaryauthors']
    except KeyError as e:
        pass

    zooniverseBoilerplate = ''
    try:
        zooniverseBoilerplate = config['tns_api']['ps1']['zooniverse_boilerplate']
    except KeyError as e:
        pass

    zooniverseScoreThreshold = 0.95
    try:
        zooniverseScoreThreshold = float(config['tns_api']['ps1']['zooniverse_score_threshold'])
    except KeyError as e:
        pass

    supplementaryAuthorsTrigger = ''
    try:
        supplementaryAuthorsTrigger = config['tns_api']['ps1']['supplementaryauthorstrigger']
    except KeyError as e:
        pass

    reportingGroupId = GROUP
    try:
        reportingGroupId = int(config['tns_api']['ps1']['reporting_group_id'])
    except KeyError as e:
        pass

    discoveryDataSourceId = GROUP
    try:
        discoveryDataSourceId = int(config['tns_api']['ps1']['discovery_data_source_id'])
    except KeyError as e:
        pass

    if options.live:
        tnsBaseURL = config['tns_api']['ps1']['live']['baseurl']
        tnsApiKey = config['tns_api']['ps1']['live']['api_key']
        botId = config['tns_api']['ps1']['live']['bot_id']
        botName = config['tns_api']['ps1']['live']['bot_name']
    else:
        tnsBaseURL = config['tns_api']['ps1']['sandbox']['baseurl']
        tnsApiKey = config['tns_api']['ps1']['sandbox']['api_key']
        botId = config['tns_api']['ps1']['sandbox']['bot_id']
        botName = config['tns_api']['ps1']['sandbox']['bot_name']

    if options.getreports:
        names = getSubmissionReports(conn, tnsBaseURL, tnsApiKey, botId = botId, botName = botName)
        if not names:
            sys.stderr.write("Bad response. Looks like the report does not exist yet, or there are no reports to request.\n")
        else:
            reports = updateTNSNames(conn, names)
            if reports:
                # Set the relevant request ID to be completed
                for reportId in set(reports):
                    tnsUpdateRequestStatus(conn, reportId, COMPLETE)

    else:
        objectList = []

        if len(args) > 1:
            objectIds = []
            for i in range(1,len(args)):
                try:
                    objectIds.append(int(args[i]))
                except ValueError:
                    sys.stderr.write("Object IDs must be integers.\n")
                    sys.exit(1)
            objectList = getSpecifiedObjects(conn, objectIds)
        else:
            if options.customlist is not None:
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
                        objectList = getObjectsByList(conn, detectionList, dateThreshold = flagDate)
                    else:
                        sys.stderr.write("The list must be between 0 and 6 inclusive.  Exiting.")
                        sys.exit(1)

        if len(objectList) > 0:
            if len(objectList) > 1 and zooniverseUsers:
                sys.stderr.write("Zooniverse remarks cannot be attached to more than one object. Please report ONE object at a time.\n")
                sys.exit(1)
            reports = tnsReport(conn, tnsBaseURL, tnsApiKey, objectList, skipNonDetections = options.skipnondetections, zooniverseUsers = zooniverseUsers, proprietaryPeriod = options.proprietaryperiod, proprietaryUnits = proprietaryUnits, kegs = options.kegs, reporter = tnsAuthors, supplementaryAuthors = supplementaryAuthors, supplementaryAuthorsTrigger = supplementaryAuthorsTrigger, reportingGroupId = reportingGroupId, discoveryDataSourceId = discoveryDataSourceId, zooniverseBoilerplate = zooniverseBoilerplate, zooniverseScoreThreshold = zooniverseScoreThreshold, botId = botId, botName = botName, addInternalIDs = options.internalids)
            for row in reports:
                print("TNS report ID = %s" % row)
        else:
            print("No objects to report.")

    return 0


if __name__ == '__main__':
    main()
