#!/usr/bin/env python
"""Request Pan-STARRS Postage Stamps

Usage:
  %s <configFile> [<candidate>...] [--test] [--listid=<listid>] [--customlist=<customlistid>] [--flagdate=<flagdate>] [--limit=<limit>] [--limitdays=<limitdays>] [--limitdaysafter=<limitdaysafter> ] [--usefirstdetection] [--overrideflags] [--requestprefix=<requestprefix>] [--requesthome=<requesthome>] [--detectiontype=(all|detections|nondetections)] [--requesttype=(all|incremental)] [--nprocesses=<nprocesses>] [--loglocation=<loglocation>]
  %s (-h | --help)
  %s --version

Options:
  -h --help                                          Show this screen.
  --version                                          Show version.
  --test                                             Just do a quick test.
  --listid=<listid>                                  Object list id
  --customlist=<customlistid>                        The object custom list
  --flagdate=<flagdate>                              Date threshold - no hyphens [default: 20200101]
  --limit=<limit>                                    Recurrent limit - how many stamps will we request
  --limitdays=<limitdays>                            Number of days before which we will not request forced photometry [default: 100]
  --limitdaysafter=<limitdaysafter>                  Number of days after which we will not request images [default: 0]
  --usefirstdetection                                Use the first detection from which to count date threshold
  --overrideflags                                    Ignore processing flags when requesting object data. Dangerous!
  --requestprefix=<requestprefix>                    Stamp request prefix [default: qub_pstamp_request]
  --requesthome=<requesthome>                        Place to store the FITS request before sending [default: /tmp]
  --detectiontype=(all|detections|nondetections)     Detecton type [default: detections]
  --requesttype=(all|incremental)                    Request type [default: incremental]
  --nprocesses=<nprocesses>                          Number of processes to use [default: 8]
  --loglocation=<loglocation>                        Log file location [default: /tmp/]

Example:
  python %s ../../../../config/config.yaml 1124922100042044700 --requestprefix=qub_stamp_request --test
  python %s ../../../../../ps13pi/config/config.yaml 1232123421115632400 --requesttype=incremental --detectiontype=all --limitdays=6000 --usefirstdetection --limitdaysafter=6000 --overrideflags --requesthome=/db0/ingest/pstamp/requests --test
  python %s ../../../../../ps13pi/config/config.yaml --listid=4 --requesttype=incremental --detectiontype=all --limitd=6 --requesthome=/db0/ingest/pstamp/requests --test
"""

import sys
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt

sys.path.append('../../common/python')
from queries import getPanSTARRSCandidates, updateTransientObservationAndProcessingStatus, insertTransientObjectComment, LC_NON_DET_AND_BLANKS_QUERY, LC_DET_QUERY

from gkutils.commonutils import dbConnect, getCurrentMJD, PROCESSING_FLAGS, Struct, cleanOptions, splitList, parallelProcess
from pstamp_utils import getAverageCoordinates, IPP_IDET_NON_DETECTION_VALUE, writeFITSPostageStampRequestById, addRequestToDatabase, addRequestIdToTransients, sendPSRequest, updateRequestStatus, width, height, SUBMITTED, findIdCombinationForPostageStampRequest2, getObjectsByList

import MySQLdb, sys, datetime, time

OBJECTTYPES = { 'all': -1,
                'orphan': 1,
                'variablestar': 2,
                'nt': 4,
                'agn': 8,
                'sn': 16 }

DETECTIONTYPES = { 'all': 1, 'detections': 2, 'nondetections': 3 }

REQUESTTYPES = { 'all': 1, 'incremental': 2}


#OBJECTS_PER_ITERATION = 5

# 2013-10-15 KWS Order by followup_id descending so that new transients get requested first. Asking
#                for older transients chokes the postage stamp server, so better do these last.
# 2016-06-07 KWS Pass in the relevant flag. We want to be able to request stamps for an object with
#                no stamps, or request (detections and) non-detections for objects with stamps.
# 2025-08-06 KWS Never request data for an object where sherlockClassification is NULL or VS or BS.
def getPS1Candidates(conn, listId = 4, flagDate = '2015-02-01', ignoreProcessingFlags = False, processingFlags = PROCESSING_FLAGS['stamps']):

    if ignoreProcessingFlags:
        processingFlags = 0
    else:
        processingFlags = PROCESSING_FLAGS['stamps']

    try:
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute ("""
            select o.id, o.followup_id, o.ra_psf 'ra', o.dec_psf 'dec', o.local_designation 'name', o.ps1_designation, o.object_classification, o.local_comments
            from tcs_transient_objects o
           where o.followup_id is not null
             and o.detection_list_id = %s
             and o.detection_list_id != 0
             and o.followup_flag_date >= %s
             and o.sherlockClassification is not null
             and o.sherlockClassification not in ('BS', 'VS')
             and (processing_flags & %s = 0 or processing_flags is null)
        order by o.followup_id desc
        """, (listId, flagDate, processingFlags))

        resultSet = cursor.fetchall ()
        cursor.close ()

    except MySQLdb.Error as e:
        print("Error %d: %s" % (e.args[0], e.args[1]))
        sys.exit (1)

    return resultSet


def getObjectComments(conn, objectId):
   """
   This method is required for those command-line passed object IDs. We need to know the associated comments.
   """

   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      cursor.execute ("""
          select local_comments
            from tcs_transient_objects
           where id = %s
      """, (objectId,))
      resultSet = cursor.fetchone ()

      cursor.close ()

   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))
      sys.exit (1)

   return resultSet



def getNonDetectionImages(conn, objectId, ippIdet = IPP_IDET_NON_DETECTION_VALUE):
    """
    Get all the images associated with an object that are not detections
    """

    try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      cursor.execute ("""
          select *
            from tcs_postage_stamp_images
           where image_filename like '%s%%\_%s\_%%'
      """, (objectId, ippIdet))
      resultSet = cursor.fetchall ()

      cursor.close ()

    except MySQLdb.Error as e:
        print("Error %d: %s" % (e.args[0], e.args[1]))
        sys.exit (1)

    return resultSet


def getDetectionImages(conn, objectId, ippIdet = IPP_IDET_NON_DETECTION_VALUE):
    """
    Get all the images associated with an object that are not non-detections
    """

    try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      cursor.execute ("""
          select *
            from tcs_postage_stamp_images
           where image_filename like '%s%%'
             and image_filename not like '%s%%\_%s\_%%'
      """, (objectId, objectId, ippIdet))
      resultSet = cursor.fetchall ()

      cursor.close ()

    except MySQLdb.Error as e:
        print("Error %d: %s" % (e.args[0], e.args[1]))
        sys.exit (1)

    return resultSet



# 2013-10-11 KWS Added detections so that we can tie them to existing images
# 2014-07-08 KWS Added extra filter definitions. (Need to fix the query code.)
def getLightcurveDetections(conn, candidate, filters = "grizywxBV", limit = 0):

   try:
       cursor = conn.cursor(MySQLdb.cursors.DictCursor)
       cursor.execute (LC_DET_QUERY, (candidate,
                                      filters[0], filters[1], filters[2], filters[3], filters[4], filters[5], filters[6], filters[7], filters[8],
                                      candidate,
                                      filters[0], filters[1], filters[2], filters[3], filters[4], filters[5], filters[6], filters[7], filters[8]))
       resultSet = cursor.fetchall ()

       if limit > 0 and len(resultSet) >= limit:
           resultSet = resultSet[-limit:]
       cursor.close ()

   except MySQLdb.Error as e:
       print("Error %d: %s" % (e.args[0], e.args[1]))
       return ()

   return resultSet


# 2014-07-08 KWS Added extra filter definitions. (Need to fix the query code.)
def getLightcurveNonDetectionsAndBlanks(conn, candidate, filters="grizywxBV", ippIdetBlank = IPP_IDET_NON_DETECTION_VALUE):

   #modifiedResultSet = []

   try:
       cursor = conn.cursor(MySQLdb.cursors.DictCursor)
       cursor.execute (LC_NON_DET_AND_BLANKS_QUERY, (candidate,
                                                     candidate,
                                                     candidate,
                                                     candidate,
                                                     filters[0], filters[1], filters[2], filters[3], filters[4], filters[5], filters[6], filters[7], filters[8]))
       resultSet = cursor.fetchall ()
       cursor.close ()

   except MySQLdb.Error as e:
       print("Error %d: %s" % (e.args[0], e.args[1]))
       return ()

   # Add the ipp_idet blank value.  Too complicated to add this to the query.
   ra, dec = getAverageCoordinates(conn, candidate)
   for row in resultSet:
       row['id'] = candidate
       row['ra_psf'] = ra
       row['dec_psf'] = dec
       row['ipp_idet'] = ippIdetBlank

   return resultSet


def getExistingNonDetectionImages(conn, candidate, ippIdetBlank = IPP_IDET_NON_DETECTION_VALUE):

    imageGroups = []

    try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      cursor.execute ("""
          select name
            from tcs_image_groups
           where name like '%s%%\_%s'
      """, (candidate, ippIdetBlank))
      resultSet = cursor.fetchall ()

      cursor.close ()

    except MySQLdb.Error as e:
        print("Error %d: %s" % (e.args[0], e.args[1]))
        sys.exit (1)

    if resultSet:
        imageGroups = [x['name'] for x in resultSet]

    return imageGroups



def getExistingDetectionImages(conn, candidate, ippIdetBlank = IPP_IDET_NON_DETECTION_VALUE):

    imageGroups = []

    try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      cursor.execute ("""
          select name
            from tcs_image_groups
           where name like '%s%%'
             and name not like '%s%%\_%s'
      """, (candidate, candidate, ippIdetBlank))
      resultSet = cursor.fetchall ()

      cursor.close ()

    except MySQLdb.Error as e:
        print("Error %d: %s" % (e.args[0], e.args[1]))
        sys.exit (1)

    if resultSet:
        imageGroups = [x['name'] for x in resultSet]

    return imageGroups


def eliminateExistingImages(conn, candidate, detections, detectionsWithImages):
    """
    We'd really like to avoid requesting images that we already have.
    """

    imagesToRequest = []

    for row in detections:
        if '%d_%s_%d_%d' % (candidate, row['tdate'], row['imageid'], row['ipp_idet']) not in detectionsWithImages:
            imagesToRequest.append(row)

    return imagesToRequest


# 2015-03-13 KWS Remove any lightcurve points before the specified date threshold.
#                Most of the itme it's just unnecessary to request non-detections
#                from (e.g.) 100 days ago!  By default we won't bother requesting data
#                older than 50 days.  If thresholdMJD is None, we will eliminate data
#                older than 50 days from NOW.  If thresholdMJD is set, we will
#                eliminate data older than 50 days before detection.
def eliminateOldDetections(conn, candidate, detections, thresholdMJD, thresholdMJDMax = 70000):
    """
    Remove lightcurve points from before our date threshold
    """

    imagesToRequest = []

    for row in detections:
        try:
            if float(row['tdate']) >= thresholdMJD and float(row['tdate']) <= thresholdMJDMax:
                imagesToRequest.append(row)
        except ValueError as e:
            print(e)

    return imagesToRequest
   

def requestStamps(conn, options, candidateList, objectsPerIteration, stampuser, stamppass, stampemail, requestHome = '/tmp', uploadURL = None, n = None):

    camera = 'gpc1'
    limit = 0
    if options.limit is not None:
        limit = int(options.limit)
    limitDays = int(options.limitdays)
    limitDaysAfter = int(options.limitdaysafter)
    useFirstDetection = options.usefirstdetection

    processingFlags = PROCESSING_FLAGS['stamps']

    detectionType = options.detectiontype
    if detectionType in ('nondetections', 'all'):
        processingFlags = PROCESSING_FLAGS['nondets']

    requestType = options.requesttype

    # We need to split our requests so that the postage stamp server can handle them efficiently
    arrayLength = len(candidateList)
    maxNumberOfCandidates = objectsPerIteration
    numberOfIterations = int(arrayLength/maxNumberOfCandidates)

    # Check to see if we need an extra iteration to clean up the end of the array
    if arrayLength%maxNumberOfCandidates != 0:
        numberOfIterations += 1

    print("Number of iterations = %d" % numberOfIterations)

    for currentIteration in range(numberOfIterations):
        candidateArray = candidateList[currentIteration*maxNumberOfCandidates:currentIteration*maxNumberOfCandidates+maxNumberOfCandidates]
        print("Iteration %d / %d" % (currentIteration + 1, numberOfIterations))

        imageRequestData = []
        for candidate in candidateArray:

            if detectionType == 'all' and limit == 0:
                lightcurveData = getLightcurveDetections(conn, candidate['id'])
                lightcurveData += getLightcurveNonDetectionsAndBlanks(conn, candidate['id'])
                existingImages = getExistingDetectionImages(conn, candidate['id'])
                existingImages += getExistingNonDetectionImages(conn, candidate['id'])
            elif detectionType == 'nondetections' and limit == 0:
                lightcurveData = getLightcurveNonDetectionsAndBlanks(conn, candidate['id'])
                existingImages = getExistingNonDetectionImages(conn, candidate['id'])
            elif detectionType == 'detections' and limit == 0:
                lightcurveData = getLightcurveDetections(conn, candidate['id'])
                existingImages = getExistingDetectionImages(conn, candidate['id'])
            else:
                # Assume a limit is set, otherwise no request will get sent. Setting a limit
                # forces only detections to be requested and this will always force the most
                # most recent <limit> images to be requested.
                if limit > 0:
                    lightcurveData = getLightcurveDetections(conn, candidate['id'], limit = limit)

            if requestType == 'incremental' and limit == 0:
                lightcurveData = eliminateExistingImages(conn, candidate['id'], lightcurveData, existingImages)

            if limitDays > 0:
                thresholdMJDMax = 70000
                if useFirstDetection:
                    # We need to know when the first detection was,
                    # but we don't always request detections.
                    detectionData = getLightcurveDetections(conn, candidate['id'])
                    # The detection MJD should be the first element returned
                    thresholdMJD = detectionData[0]['mjd'] - limitDays
                    if limitDaysAfter > 0:
                        thresholdMJDMax = detectionData[0]['mjd'] + limitDaysAfter
                else:
                    thresholdMJD = getCurrentMJD() - limitDays
                lightcurveData = eliminateOldDetections(conn, candidate['id'], lightcurveData, thresholdMJD, thresholdMJDMax)

            if lightcurveData:
                #for row in lightcurveData:
                #    print row
                imageRequestData += lightcurveData

        print("Number of PS Images to Request: %d" % len(imageRequestData))

        currentDate = datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")
        (year, month, day, hour, min, sec) = currentDate.split(':')
        timeReqeustSuffix = "%s%s%s_%s%s%s" % (year, month, day, hour, min, sec)

        requestName = "%s_%s" % (options.requestprefix, timeReqeustSuffix)

        # Are we in multiprocessing mode?
        if n is not None:
            requestName += '_%02d' % n

        requestFileName = "%s/%s.fits" % (requestHome, requestName)

        # 2015-09-30 KWS Minor bug fix. Should be referring to imageRequestData
        #                below, not lightcurveData.
        if options.test:
            if imageRequestData:
                for row in imageRequestData:
                    print(row['mjd'], row['filter'], row['fpa_detector'], end=' ')
                    diffImageCombination = findIdCombinationForPostageStampRequest2(row)
                    for imType in ['target','ref','diff']:
                        print("%s (%s): %s" % (imType, diffImageCombination[imType][0], diffImageCombination[imType][1]), end=' ')
                    print()
            print("Just testing...  No requests sent.")
            continue

        if not len(imageRequestData):
            print("No detections to request.  Skipping this iteration.")
            continue

        writeFITSPostageStampRequestById(conn, requestFileName, requestName, imageRequestData, width, height, email = stampemail, camera=camera)

        # Temporarily stop processing here (i.e. don't send the FITS file to the PSS).
        #time.sleep(1)
        #continue

        # Extract the candidates into a list so that we don't have to rewrite any code
        candidateIdList = [x['id'] for x in candidateArray]

        sqlCurrentDate = "%s-%s-%s %s:%s:%s" % (year, month, day, hour, min, sec)

        psRequestId = addRequestToDatabase(conn, requestName, sqlCurrentDate)
        if (psRequestId > 0):
            print("Postage Stamp Request ID = %d" % psRequestId)

            # Send the request to the postage stamp server
            pssServerId = sendPSRequest(requestFileName, requestName, username = stampuser, password = stamppass, postageStampServerURL = uploadURL)
            if (pssServerId >= 0):
                addRequestIdToTransients(conn, psRequestId, candidateIdList, processingFlag = processingFlags)
                submitted = updateRequestStatus(conn, requestName, SUBMITTED, pssServerId)
                # 2024-04-24 KWS For reasons I DO NOT UNDERSTAND, 50% of requests get submitted twice. WHY????
                #                Added an extra commit in an attempt to STOP this happening, but I don't know
                #                why it's needed.
                conn.commit()
                if (submitted > 0):
                    print("Successfully submitted job to Postage Stamp Server and updated database.")
                else:
                    print("Submitted job, but did not update database.")
            else:
                print("Did Not successfully submit the job to the Postage Stamp Server!")
                # Remove the request ID from the transients - we can try again some other time.
                addRequestIdToTransients(conn, None, candidateIdList, processingFlag = PROCESSING_FLAGS['unprocessed'])


        # Sleep for 1 second.  This should ensure that all request IDs (which are time based) are unique
        time.sleep(1)

    return candidateList


def workerStampRequester(num, db, listFragment, dateAndTime, firstPass, miscParameters):
    """thread worker function"""
    # Redefine the output to be a log file.
    options = miscParameters[0]
    objectsPerIteration = miscParameters[1]
    stampuser = miscParameters[2]
    stamppass = miscParameters[3]
    email = miscParameters[4]
    requestHome = miscParameters[5]
    uploadURL = miscParameters[6]

    sys.stdout = open('%s%s_%s_%d.log' % (options.loglocation, options.logprefix, dateAndTime, num), "w")
    conn = dbConnect(db[3], db[0], db[1], db[2])
    # 2023-03-13 KWS MySQLdb disables autocommit by default. Switch it on globally.
    conn.autocommit(True)

    # Call the postage stamp requester
    objectsForUpdate = requestStamps(conn, options, listFragment, objectsPerIteration, stampuser, stamppass, email, requestHome = requestHome, uploadURL = uploadURL, n = num)
    #q.put(objectsForUpdate)

    print("Process complete.")
    conn.close()
    print("DB Connection Closed - exiting")
    return 0




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
        config = yaml.load(yaml_file, Loader=yaml.SafeLoader)

    username = config['databases']['local']['username']
    password = config['databases']['local']['password']
    database = config['databases']['local']['database']
    hostname = config['databases']['local']['hostname']

    stampuser = config['web_credentials']['stampserver']['username']
    stamppass = config['web_credentials']['stampserver']['password']

    test = options.test

    # The default is to request all days of data.
    limitDays = int(options.limitdays)
    limitDaysAfter = int(options.limitdaysafter)
    useFirstDetection = options.usefirstdetection

    processingFlags = PROCESSING_FLAGS['stamps']

    detectionType = options.detectiontype

    if detectionType in ('nondetections', 'all'):
        processingFlags = PROCESSING_FLAGS['nondets']

    MAX_NUMBER_OF_OBJECTS = int(config['postage_stamp_parameters']['max_number_of_objects'])
    OBJECTS_PER_ITERATION = int(config['postage_stamp_parameters']['objects_per_iteration'])

    email = config['postage_stamp_parameters']['email']
    camera = config['postage_stamp_parameters']['camera']
    uploadURL = config['postage_stamp_parameters']['uploadurl']

    if options.requesthome:
        requestHome = options.requesthome
    else:
        requestHome = '/' + os.uname()[1].split('.')[0] + '/ingest/pstamp/requests'

    conn = dbConnect(hostname, username, password, database)
    if not conn:
        print("Cannot connect to the database")
        return 1

    # 2023-04-19 KWS MySQLdb disables autocommit by default. Switch it on globally.
    conn.autocommit(True)

    if options.limit is not None:
        try:
            limit = int(options.limit)
            if limit < 2 or limit > 100:
                print("Detection limit must be between 2 and 100")
                return 1
        except ValueError as e:
            sys.exit("Detection limit must be an integer")


    if options.flagdate is not None:
        try:
            flagDate = '%s-%s-%s' % (options.flagdate[0:4], options.flagdate[4:6], options.flagdate[6:8])
        except:
            flagDate = '2015-02-01'


    candidateList = []

    nprocesses = int(options.nprocesses)

    # If the list isn't specified assume it's the Eyeball List.
    if options.listid is not None:
        try:
            detectionList = int(options.listid)
            if detectionList < 0 or detectionList > 8:
                print ("Detection list must be between 0 and 8")
                return 1
        except ValueError as e:
            sys.exit("Detection list must be an integer")

    dateThreshold = '%s-%s-%s' % (options.flagdate[0:4], options.flagdate[4:6], options.flagdate[6:8])

    candidateList = []
    if len(options.candidate) > 0:
        for row in options.candidate:
            object = getObjectsByList(conn, objectId = int(row))
            if object:
                candidateList.append(object)

    else:
        candidateList = getObjectsByList(conn, listId = detectionList, dateThreshold = dateThreshold, processingFlags = processingFlags)
        #candidateList = getPS1Candidates(conn, listId = detectionList, flagDate = flagDate, processingFlags = processingFlags, ignoreProcessingFlags = options.overrideflags)


    print("TOTAL OBJECTS = %d" % len(exposureSet))
    if len(candidateList) > MAX_NUMBER_OF_OBJECTS:
        sys.exit("Maximum request size is for images for %d candidates. Attempted to make %d requests.  Aborting..." % (MAX_NUMBER_OF_OBJECTS, len(candidateList)))

    if nprocesses == 1:
        requestStamps(conn, options, candidateList, OBJECTS_PER_ITERATION, stampuser, stamppass, email, requestHome = requestHome, uploadURL = uploadURL)

    else:
        # Do some multiprocessing.
        print("Requesting stamps...")
    
        if len(candidateList) > 1:
            nProcessors, listChunks = splitList(candidateList, bins = nprocesses)

            print("%s Parallel Processing..." % (datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")))
            parallelProcess(db, dateAndTime, nProcessors, listChunks, workerStampRequester, miscParameters = [options, OBJECTS_PER_ITERATION, stampuser, stamppass, email, requestHome, uploadURL], drainQueues = False)
            print("%s Done Parallel Processing" % (datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")))



    conn.commit ()
    conn.close ()


if __name__ == '__main__':
    main()
