#!/usr/bin/env python
from tnsAPI import addBulkReport, getBulkReportReply
import sys
import logging

# The following code should be useable by both the Pan-STARRS and ATLAS surveys.

# Use the TNS logger /tmp/tns.log to record log info
logger = logging.getLogger(__name__)

# 2025-07-07 KWS Added ATLAS-TDO definitions.
ATLASFilters = {'c': '71', 'o': '72', 'w': '73'}
ATLASInstrument = {'02a': '159', '01a': '160', '03a': '255', '04a': '256', '05r': '290'}
ATLASGroup = 18
ATTYPES = {'PSN': 1, 'PNV': 2, 'AGN': 3, 'Other': 0}

SUBMITTED           = 1
COMPLETE            = 2
COMMUNICATION_ERROR = 3
TIMEOUT             = 4
DOWNLOADING         = 5
CORRUPT             = 7

def tnsAddRequestToDatabase(conn, reportId):
    """
    Add the TNS request to the database

    :param conn: database connection
    :param reportId:
    :return: insert id

    """
    import MySQLdb

    try:
        cursor = conn.cursor (MySQLdb.cursors.DictCursor)

        cursor.execute ("""
             insert into tcs_tns_requests (tns_report_id, download_attempts, status, created)
             values (%s, %s, %s, now())
             """, (reportId, 0, SUBMITTED))

    except MySQLdb.Error as e:
        sys.stderr.write("Error %d: %s\n" % (e.args[0], e.args[1]))

    cursor.close ()
    return conn.insert_id()


def tnsUpdateRequestStatus(conn, reportId, status):
    """
    Update the TNS request status

    :param conn: database connection
    :param reportId:
    :param status: an integer - see above

    """
    import MySQLdb

    rowsUpdated = 0
    try:
        cursor = conn.cursor (MySQLdb.cursors.DictCursor)

        cursor.execute ("""
              update tcs_tns_requests
              set status = %s,
              updated = now()
              where tns_report_id = %s
              """, (status, reportId))
        rowsUpdated = cursor.rowcount
        cursor.close ()

    except MySQLdb.Error as e:
        sys.stderr.write("Error %d: %s\n" % (e.args[0], e.args[1]))

    return rowsUpdated


def tnsUpdateRequestDownloadAttempts(conn, reportId, status):
    """
    Update the number of times we have attempted to download the TNS response

    :param conn: database connection
    :param reportId:
    :param status: an integer - see above

    """
    import MySQLdb
    rowsUpdated = 0
    try:
        cursor = conn.cursor (MySQLdb.cursors.DictCursor)

        cursor.execute ("""
             update tcs_tns_requests
             set download_attempts = download_attempts+1,
             status = %s,
             updated = now()
             where tns_report_id = %s
             """, (status, reportId))


    except MySQLdb.Error as e:
        sys.stderr.write("Error %d: %s\n" % (e.args[0], e.args[1]))

    rowsUpdated = cursor.rowcount
    cursor.close ()

    return rowsUpdated


def tnsGetRequestList(conn, status, creationDate = '2016-04-01 00:00:00', limit = None):
    """
    Get TNS requests with specified status

    :param conn: database connection
    :param status: an integer - see above
    :param creationDate:  (Default value = '2016-04-01 00:00:00')
    :param limit:  (Default value = None)
    :return psRequestList: TNS request list

    """
    import MySQLdb
    try:
        cursor = conn.cursor (MySQLdb.cursors.DictCursor)

        if limit:
            cursor.execute ("""
                select * from tcs_tns_requests
                where status = %s
                and created > %s
                limit %s
            """, (status, creationDate, limit))
        else:
            cursor.execute ("""
                select * from tcs_tns_requests
                where status = %s
                and created > %s
            """, (status, creationDate))

        psRequestList = cursor.fetchall ()
        cursor.close ()

    except MySQLdb.Error as e:
        sys.stderr.write("Error %d: %s\n" % (e.args[0], e.args[1]))
        sys.exit (1)

    return psRequestList


def getSubmissionReports(conn, tnsBaseURL, tnsApiKey, botId = None, botName = None):
    """
    Get all the reports back from the TNS that have not yet been
    processed and return a list of tns names with internal names
    and whether or not the object exists on the nameserver.

    :param conn: database connection
    :param tnsBaseURL: TNS base URL
    :param tnsApiKey: TNS API Key
    :return names: dict containing internal name, tns name, bool exists, tns report id

    """

    names = []
    tnsRequests = tnsGetRequestList(conn, SUBMITTED)
    if tnsRequests:
        for r in tnsRequests:
            # 2025-01-27 KWS Major changes in 2025. We will no longer receive the originating request as part of the response.
            #                The only reason we parse it was to tie up the TNS name with ATLAS or Pan-STARRS internal name.
            #                In the new regime, we will get an "internal_ids" key made up of the user defined key value pairs
            #                we sent in our request.
            request, response = getBulkReportReply(r['tns_report_id'], tnsBaseURL, tnsApiKey, botId = botId, botName = botName)
            if request is None:
                # We have now reverted to placing internal_ids data into the response
                pass
            else:    
                logger.debug("ORIGINAL REQUEST is BELOW")
                logger.debug(json.dumps(request, indent=4, sort_keys=True))

            logger.debug("RESPONSE is BELOW")
            logger.debug(json.dumps(response, indent=4, sort_keys=True))

            # 2025-01-29 KWS New code to deal with the new internal_ids.

            if request is None and response is not None:
                # We have some kind of response back from the TNS, which no longer includes the original request.
                if type(response) is list:
                    for row in response:
                        try:
                            names.append({'objname': row['100']['objname'],
                                          'internal_name': row['internal_ids']['internal_name'],
                                          'internal_id': row['internal_ids']['internal_objid'],
                                          'exists': False, 'report_id': r['tns_report_id']})
                        except KeyError as e:
                            if '100' in str(e):
                                try:
                                    names.append({'objname': row['101']['objname'],
                                                  'internal_name': row['internal_ids']['internal_name'],
                                                  'internal_id': row['internal_ids']['internal_objid'],
                                                  'exists': True, 'report_id': r['tns_report_id']})
                                except KeyError as e:
                                    logger.error("One of the relevant keys (%s) is missing." % str(e))

            elif request is not None and response is not None:
                # We got our old request, so read the data the old way.
                if type(request) is list and type(response) is list and len(response) == len(request):
                    # Process the data as a list. Could be one or more objects.
                    for i in range(len(response)):
                        try:
                            names.append({'objname': response[i]['100']['objname'],
                                          'internal_name': request[i]['internal_name'],
                                          'exists': False, 'report_id': r['tns_report_id']})
                        except KeyError as e:
                            if '100' in str(e):
                                try:
                                    names.append({'objname': response[i]['101']['objname'],
                                                  'internal_name': request[i]['internal_name'],
                                                  'exists': True, 'report_id': r['tns_report_id']})
                                except KeyError as e:
                                    logger.error("Cannot find the relevant message. Must be 100 or 101. Got %s." % str(e))

                elif type(request) is dict and type(response) is dict and len(list(request.keys())) == len(list(response.keys())):
                    for k, v in request.items():
                        try:
                            names.append({'objname': response[k]['100']['objname'],
                                          'internal_name': request[k]['internal_name'],
                                          'exists': False, 'report_id': r['tns_report_id']})
                        except KeyError as e:
                            if '100' in str(e):
                                # The object already exists, so append the existing name
                                try:
                                    names.append({'objname': response[k]['101']['objname'],
                                                  'internal_name': request[k]['internal_name'],
                                                  'exists': True, 'report_id': r['tns_report_id']})
                                except KeyError as e:
                                    logger.error("Cannot find the relevant message. Must be 100 or 101. Got %s." % str(e))
                                    #names = []
                            else:
                                logger.error("Cannot find the relevant message. Must be 100 or 101.")
                                #names = []
                else:
                    logger.error("Request or Response missing! The report may have expired.")
            else:
                logger.error("Unexpected request/response combination.")

    else:
        print("No reports to process.")

    return names
