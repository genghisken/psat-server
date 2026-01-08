#!/usr/bin/env python
"""Request Pan-STARRS forced photometry

Usage:
  %s <configFile> [<candidate>...] [--test] [--listid=<listid>] [--customlist=<customlistid>] [--flagdate=<flagdate>] [--limitdays=<limitdays>] [--limitdaysafter=<limitdaysafter> ] [--usefirstdetection] [--overlapdays=<overlapdays>] [--overrideflags] [--difftype=<difftype>] [--requestprefix=<requestprefix>] [--requesthome=<requesthome>] [--rbthreshold=<rbthreshold>] [--camera=<camera>] [--coords=<coords>] [--nprocesses=<nprocesses>] [--loglocation=<loglocation>] [--logprefix=<logprefix>]
  %s (-h | --help)
  %s --version

Options:
  -h --help                           Show this screen.
  --version                           Show version.
  --test                              Just do a quick test.
  --listid=<listid>                   Object list id
  --customlist=<customlistid>         The object custom list
  --flagdate=<flagdate>               Date threshold - no hyphens [default: 20200101]
  --limitdays=<limitdays>             Number of days before which we will not request forced photometry [default: 100]
  --limitdaysafter=<limitdaysafter>   Number of days after which we will not request images [default: 0]
  --usefirstdetection                 Use the first detection from which to count date threshold
  --overlapdays=<overlapdays>         Go back this number of days before the max of what is already in the forced phot table [default: 0]
  --overrideflags                     Ignore processing flags when requesting object data. Dangerous!
  --difftype=<difftype>               Diff type [default: WSdiff]
  --requestprefix=<requestprefix>     Detectability request prefix [default: qub_det_request]
  --requesthome=<requesthome>         Place to store the FITS request before sending
  --rbthreshold=<rbthreshold>         Only request forced photometry if RB factor above a specified threshold (only applies to lists).
  --camera=<camera>                   Pan-STARRS camera [default: gpc1]
  --coords=<coords>                   Coordinates to use to override the coordinates of the specified object. Comma separated, no spaces.
  --nprocesses=<nprocesses>           Number of processes to use [default: 4]
  --loglocation=<loglocation>         Log file location [default: /tmp/]
  --logprefix=<logprefix>             Log prefix [default: forced_phot_requester]


Example:
  python %s ../../../../config/config.yaml 1124922100042044700 --requestprefix=yse_det_request --test
"""
import sys
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt
import os, MySQLdb, shutil, re, datetime, time
from gkutils.commonutils import find, Struct, cleanOptions, getCurrentMJD, readGenericDataFile, dbConnect, coords_sex_to_dec, PROCESSING_FLAGS
from pstamp_utils import getObjectsByList, writeDetectabilityFITSRequest, addRequestToDatabase, sendPSRequest, updateRequestStatus, DETECTABILITY_REQUEST, SUBMITTED
import random

def requestForcedPhotometry(conn, options, candidateList, objectsPerIteration, stampuser, stamppass, stampemail, requestHome = '/tmp', uploadURL = None, n = None):

    limitDays = int(options.limitdays)
    limitDaysAfter = int(options.limitdaysafter)
    useFirstDetection = options.usefirstdetection

    # n is an empty string or a str(process id). Used in multiprocessing mode to distinguish requests.

    # 2020-05-14 KWS Only request forced photometry for candidates that have an RB factor above
    #                a specified threshold.
    candidateListFiltered = []
    if options.rbthreshold:
        rbThreshold = float(options.rbthreshold)
        for candidate in candidateList:
            if candidate['rb_factor'] is not None and candidate['rb_factor'] >= rbThreshold:
                candidateListFiltered.append(candidate)
        candidateList = candidateListFiltered

    arrayLength = len(candidateList)
    maxNumberOfCandidates = objectsPerIteration
    numberOfIterations = int(arrayLength/maxNumberOfCandidates)
 
    # 2020-02-06 KWS Randomly shuffle the list - to reduce possiblilty of race conditions
    #                at the detectability server
    candidateList = list(candidateList)
    random.shuffle(candidateList)

    coords = []
    # If we override the coordinates and the array length is 1, substitute the coordinates
    if arrayLength == 1:
        if options.coords:
            coords = [float(options.coords.split(',')[0]), float(options.coords.split(',')[1])]

    # Check to see if we need an extra iteration to clean up the end of the array
    if arrayLength%maxNumberOfCandidates != 0:
        numberOfIterations += 1
 
    print("Number of iterations = %d" % numberOfIterations)
 
    for currentIteration in range(numberOfIterations):
        candidateSubList = candidateList[currentIteration*maxNumberOfCandidates:currentIteration*maxNumberOfCandidates+maxNumberOfCandidates]
        print("Iteration %d" % (currentIteration+1))
 
        # Get current time and date info and format it as necessary
        currentDate = datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")
        (year, month, day, hour, min, sec) = currentDate.split(':')
 
        # Construct a time suffix for the request name and format the time for SQL
        timeRequestSuffix = "%s%s%s_%s%s%s" % (year, month, day, hour, min, sec)
        sqlCurrentDate = "%s-%s-%s %s:%s:%s" % (year, month, day, hour, min, sec)
 
        requestName = "%s_%s" % (options.requestprefix, timeRequestSuffix)

        # Are we in multiprocessing mode?
        if n is not None:
            requestName += '_%s' % str(n)

        requestFileName = "%s/%s.fits" % (requestHome, requestName)
 
        fileWritten = False
        for candidate in candidateSubList:
            fileWritten = writeDetectabilityFITSRequest(conn, requestFileName, requestName, candidateSubList, diffType = options.difftype, limitDays = limitDays, limitDaysAfter = limitDaysAfter, camera = options.camera, coords = coords, overlapDays = abs(float(options.overlapdays)))
 
        time.sleep(1)
 
        if options.test or fileWritten is False or uploadURL is False:
            print("No request was sent to the stamp server. This is either a test or upload URL is not set.")
            continue

        psRequestId = addRequestToDatabase(conn, requestName, sqlCurrentDate, DETECTABILITY_REQUEST)
 
        pssServerId = sendPSRequest(requestFileName, requestName, username = stampuser, password = stamppass, postageStampServerURL = uploadURL)
        print(pssServerId)
        if pssServerId is not None and (pssServerId >= 0):
            if (updateRequestStatus(conn, requestName, SUBMITTED, pssServerId) > 0):
                print("Successfully submitted job to Postage Stamp Server and updated database.")
            else:
                print("Submitted job, but did not update database.")
        else:
            print("Did Not successfully submit the job to the Postage Stamp Server!")

        # Sleep for 1 second.  This should ensure that all request IDs (which are time based) are unique
        time.sleep(1)


def workerFPRequester(num, db, listFragment, dateAndTime, firstPass, miscParameters):
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
    objectsForUpdate = requestForcedPhotometry(conn, options, listFragment, objectsPerIteration, stampuser, stamppass, email, requestHome = requestHome, uploadURL = uploadURL, n = num)

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

    uploadURL = config['postage_stamp_parameters']['uploadurl']

    stampuser = config['web_credentials']['stampserver']['username']
    stamppass = config['web_credentials']['stampserver']['password']
    email = config['postage_stamp_parameters']['email']

    conn = dbConnect(hostname, username, password, database)
    if not conn:
        print("Cannot connect to the database")
        return 1

    detectionList = 4
    processingFlags = 0

    if options.requesthome:
        requestHome = options.requesthome
    else:
        requestHome = '/' + os.uname()[1].split('.')[0] + '/ingest/pstamp/requests'

    if not os.path.exists(requestHome):
        requestHome = "/tmp"

    MAX_NUMBER_OF_OBJECTS = int(config['postage_stamp_parameters']['max_number_of_objects'])
    OBJECTS_PER_ITERATION = int(config['postage_stamp_parameters']['fp_objects_per_iteration'])

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

    if len(candidateList) > MAX_NUMBER_OF_OBJECTS:
        sys.exit("Maximum request size is for images for %d candidates. Attempted to make %d requests.  Aborting..." % (MAX_NUMBER_OF_OBJECTS, len(candidateList)))

    if int(options.nprocesses) == 1 or len(candidateList) == 1 or options.test:
        requestForcedPhotometry(conn, options, candidateList, OBJECTS_PER_ITERATION, stampuser, stamppass, email, requestHome = requestHome, uploadURL = uploadURL)

    else:
        # Do some multiprocessing.
        print("Requesting forced photometry...")

        if len(candidateList) > 1:
            nProcessors, listChunks = splitList(candidateList, bins = nprocesses)

            print("%s Parallel Processing..." % (datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")))
            parallelProcess(db, dateAndTime, nProcessors, listChunks, workerFPRequester, miscParameters = [options, OBJECTS_PER_ITERATION, stampuser, stamppass, email, requestHome, uploadURL], drainQueues = False)
            print("%s Done Parallel Processing" % (datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")))



    conn.commit()
    conn.close()

if __name__=='__main__':
    main()
