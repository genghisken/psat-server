#!/usr/bin/env python
"""Request finders for Pan-STARRS objects

Usage:
  %s <configfile> [<candidate>...] [--detectionlist=<detectionlist>] [--customlist=<customlist>] [--update] [--size=<size>] [--flagdate=<flagdate>] [--target] [--test]
  %s (-h | --help)
  %s --version

Options:
  -h --help                                   Show this screen.
  --version                                   Show version.
  --update                                    Update the database
  --detectionlist=<detectionlist>             List option
  --customlist=<customlist>                   Custom List option
  --size=<size>                               Size of the stamp in arcsec [default: 240].
  --flagdate=<flagdate>                       Flag date before which we will not request finders [default: 20230101].
  --target                                    Request the target image, not the reference image.
  --test                                      Just testing.

E.g.:
  %s ../../../../../ps13pi/config/config.yaml 1161549880293940400 --update
  %s ../../../../../ps13pi/config/config.yaml --detectionlist=2 --update
"""

import sys, os
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt

from pstamp_utils import sendPSRequest, writeFinderPostageStampRequestById, getObjectsByList, getObjectsByCustomList, getAverageCoordinates, searchRecurrentObjectsForFinder, addRequestToDatabase, FINDER_REQUEST_V2, addRequestIdToTransients, updateRequestStatus, SUBMITTED
from gkutils.commonutils import dbConnect, calculateRMSScatter, truncate, PROCESSING_FLAGS, cleanOptions, Struct
import datetime, time

import MySQLdb

requestPrefix = "qub_ps_request"
requestHome = '/' + os.uname()[1].split('.')[0] + '/ingest/pstamp/requests'

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
   options = Struct(**opts)
       
   configFile = options.configfile
   
   import yaml
   with open(configFile) as yaml_file:
       config = yaml.safe_load(yaml_file)

   flagDate = '2015-02-01'
   detectionList = 0

   imageType = 'ref'

   test = options.test

   target = options.target

   processingFlag = PROCESSING_FLAGS['reffinders']
   if options.target:
      imageType = 'target'
      processingFlag = PROCESSING_FLAGS['targetfinders']


   username = config['databases']['local']['username']
   password = config['databases']['local']['password']
   database = config['databases']['local']['database']
   hostname = config['databases']['local']['hostname']

   stampUsername = config['web_credentials']['ps1md']['username']
   stampPassword = config['web_credentials']['ps1md']['password']

   email = 'qub2@qub.ac.uk'
   camera = 'gpc1'

   try:
      email = config['postage_stamp_parameters']['email']
   except KeyError as e:
      pass

   try:
      camera = config['postage_stamp_parameters']['camera']
   except KeyError as e:
      pass

   uploadURL = config['postage_stamp_parameters']['uploadurl']
   downloadURL = config['postage_stamp_parameters']['downloadurl']

   sizeInArcsec = int(options.size)

   conn = dbConnect(hostname, username, password, database)

   # 2023-04-19 KWS MySQLdb disables autocommit by default. Switch it on globally.
   conn.autocommit(True)

   if options.flagdate is not None:
      try:
         flagDate = '%s-%s-%s' % (options.flagdate[0:4], options.flagdate[4:6], options.flagdate[6:8])
      except:
         flagDate = '2015-02-01'

   objectList = []

   flagDate = '2015-12-20'
   if options.flagdate is not None:
       try:
           flagDate = '%s-%s-%s' % (options.flagdate[0:4], options.flagdate[4:6], options.flagdate[6:8])
       except:
           flagDate = '2015-12-20'

   if options.candidate is not None and len(options.candidate) > 0:
       for cand in options.candidate:
           obj = getObjectsByList(conn, objectId = int(cand))
           if obj:
               objectList.append(obj)
   else:

       if options.customlist is not None:
           if int(options.customlist) > 0 and int(options.customlist) < 100:
               customList = int(options.customlist)
               objectList = getObjectsByCustomList(conn, customList, processingFlags = processingFlag)
           else:
               print("The list must be between 1 and 100 inclusive.  Exiting.")
               sys.exit(1)
       else:
           if options.detectionlist is not None:
               if int(options.detectionlist) > 0 and int(options.detectionlist) < 11:
                   detectionList = int(options.detectionlist)
                   objectList = getObjectsByList(conn, listId = detectionList, dateThreshold = flagDate, processingFlags = processingFlag)
               else:
                   print("The list must be between 1 and 10 inclusive.  Exiting.")
                   sys.exit(1)

   if sizeInArcsec < 60 or sizeInArcsec > 800:
      print("Size should be between 60 and 800 arcsec (1 arcmin and 13.3 arcmin).  Exiting program...")
      sys.exit(0)


   # Which recurrence do we want to use to generate the finder?  3 most recent??



   # We need to split our requests so that the postage stamp server can handle them efficiently
   arrayLength = len(objectList)
   maxNumberOfCandidates = 20
   numberOfIterations = int(arrayLength/maxNumberOfCandidates)

   # Check to see if we need an extra iteration to clean up the end of the array
   if arrayLength%maxNumberOfCandidates != 0:
      numberOfIterations += 1

   print("Number of iterations = %d" % numberOfIterations)

   for currentIteration in range(numberOfIterations):
      candidateArray = objectList[currentIteration*maxNumberOfCandidates:currentIteration*maxNumberOfCandidates+maxNumberOfCandidates]
      print("Iteration %d" % (currentIteration + 1))

      results = []
      for candidate in candidateArray:
         # Calculate AVERAGE RA and DEC and override any recurrence RA and DEC.
         (ra, dec) = getAverageCoordinates(conn, candidate['id'])
         #uniqueResultSet = searchUniqueObjectsForFinder(conn, candidate['id'], ra, dec)
         #results += uniqueResultSet
         #recurrentResultSet = searchRecurrentObjectsForFinder(conn, candidate, ra, dec)
         recurrentResultSet = searchRecurrentObjectsForFinder(conn, candidate['id'], ra, dec, limit = 2)
         results += recurrentResultSet
         #if len(recurrentResultSet) < 3:
         #   results += uniqueResultSet + recurrentResultSet
         #else:
         #   results += recurrentResultSet

      for result in results:
         print(result)

      print("Number of PS Images to Request: %d" % len(results))

      # We have the data, now construct the FITS request file

      # Get the date.  Could use "now()" when inserting into database
      # but this ensures that the request name suffix and the date in the
      # database are always identical (nice to have consistency).
      currentDate = datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")

      (year, month, day, hour, min, sec) = currentDate.split(':')
   
      timeReqeustSuffix = "%s%s%s_%s%s%s" % (year, month, day, hour, min, sec)

      requestName = "%s_%s" % (requestPrefix, timeReqeustSuffix)
      requestFileName = "%s/%s.fits" % (requestHome, requestName)

      # Request a reference stack finder (no SN present)
      # 2015-02-24 KWS No longer need to request CMF files.
      #                Use PS1 Ubercal Stars catalog instead.
      #                We will use sky2xy to get the x,y coords
      #                from the star catalog.

      if test:
         print("Just testing...  No requests sent.")
         continue

      writeFinderPostageStampRequestById(conn, None, requestFileName, requestName, results, sizeInArcsec, imageType = imageType, email = email, camera = camera)

      sqlCurrentDate = "%s-%s-%s %s:%s:%s" % (year, month, day, hour, min, sec)

      # The new V2 finders to not require the CMF.  Lets differentiate them so that we
      # can still request finders in the old way if necessary.
      psRequestId = addRequestToDatabase(conn, requestName, sqlCurrentDate, requestType=FINDER_REQUEST_V2)

      if (psRequestId > 0):
         pssServerId = sendPSRequest(requestFileName, requestName, username = stampUsername, password = stampPassword, postageStampServerURL = uploadURL)

         if (pssServerId >= 0):
            addRequestIdToTransients(conn, psRequestId, [candidate['id'] for candidate in candidateArray], processingFlag = processingFlag)
            print("Predicted URL is: %s" % downloadURL + requestName)
            if (updateRequestStatus(conn, requestName, SUBMITTED, pssServerId) > 0):
               print("Successfully submitted job to Postage Stamp Server and updated database.")
            else:
               print("Submitted job, but did not update database.")
         else:
            print("Did Not successfully submit the job to the Postage Stamp Server!")

      # Sleep for 1 second.  This should ensure that all request IDs (which are time based) are unique
      time.sleep(1)


   conn.commit ()
   conn.close ()


# ###########################################################################################
#                                         Main hook
# ###########################################################################################


if __name__=="__main__":
    main()
