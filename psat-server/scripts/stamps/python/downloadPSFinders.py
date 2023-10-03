#!/usr/bin/env python
"""Download the Pan-STARRS requested finders

Usage:
  %s <configfile> [--downloadpath=<downloadpath>] [--nsigma=<nsigma>]
  %s (-h | --help)
  %s --version

Options:
  -h --help                                   Show this screen.
  --version                                   Show version.
  --downloadpath=<downloadpath>               Temporary location of image downloads [default: /tmp].
  --nsigma=<nsigma>                           Specify a multiplier of the standard deviation to adjust the contrast [default: 2.0].

E.g.:
  %s ../../../../../ps13pi/config/config.yaml
"""

import sys
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt
from gkutils.commonutils import dbConnect, calculateRMSScatter, truncate, PROCESSING_FLAGS, cleanOptions, Struct
from pstamp_utils import getPSRequestList, insertPostageStampImageRecord, downloadFinderImage2, findDictInDictList, GROUP_TYPE_FINDER, ERROR_FILE_NOT_IN_DATASTORE_INDEX, BAD_SERVER_ADDRESS, PAGE_NOT_FOUND, HTTP_ERROR, SUBMITTED, COMPLETE, COMMUNICATION_ERROR, TIMEOUT, FINDER_REQUEST_V2, DOWNLOADING, updateRequestStatus, getDataStoreIndex, parseIndexList, updateDownloadAttempts

import os
import requests, urllib
from astropy.io import fits as pf

# 2014-03-08 KWS New code for Finders
# Created a copy of pstamp_get_results to get Finder results.

# Global constants
resultsFileLocation = '/' + os.uname()[1].split('.')[0] + '/ingest/pstamp/responses'



def downloadAndRenameResultsFile(options, resultsFileInfo, requestName, downloadURL = None):
   """downloadAndRenameResultsFile.

   Args:
       resultsFileInfo:
       requestName:
       downloadURL:
   """
   # Get the file and rename it.

   resultsURL = None
   if downloadURL is not None:
      resultsURL = downloadURL

   localResultsFile = None

   localResultsFile = resultsFileLocation + '/' + requestName + '_results.fits'

   try:
      urllib.request.urlretrieve(resultsURL + requestName +'/' + resultsFileInfo['fileID'], localResultsFile)

      #if md5(localResultsFile) != resultsFileInfo['md5sum']:
      #   print "MD5 hashes do not match.  Results file is corrupt."
      #   # Delete the results file - it's garbage
      #   os.remove(localResultsFile)
      #   updateDownloadAttempts(conn, requestName, CORRUPT)
      #   localResultsFile = None

   except IOError as e:
      print("ERROR: Could not download results file. Error is: %s" % e.errno)
      print(e)
      localResultsFile = None

   return localResultsFile


def downloadAllFinderImages(conn, options, requestName, PSSImageRootLocation, offsetStarFilter = None, downloadURL = None, nsigma = 2.0, connSherlock = None):
   """downloadAllFinderImages.

   Args:
       conn:
       options:
       requestName:
       PSSImageRootLocation:
       offsetStarFilter:
       downloadURL:
       connSherlock:
   """

   if connSherlock is None:
      connSherlock = conn

   (indexFile, errorCode) = getDataStoreIndex(requestName, dataStoreURL = downloadURL)

   if (errorCode == BAD_SERVER_ADDRESS):
      # We can't continue.  Might as well give up. Someone has moved the PSS
      print("Cannot continue...  Postage Stamp Server is not available.")
      return(1)

   if (errorCode == PAGE_NOT_FOUND):
      # Our request probably isn't ready yet
      updateDownloadAttempts(conn, requestName, TIMEOUT)
      return(1)

   if (errorCode == HTTP_ERROR):
      # Something went 'orribly wrong
      updateDownloadAttempts(conn, requestName, COMMUNICATION_ERROR)
      return(1)

   if indexFile != '':
      list = indexFile.split('\n')
      dataStoreFileInfoList = parseIndexList(list)

      # The first list of dicts should be our results file
      if dataStoreFileInfoList[0][0]['fileID'] != 'results.fits':
         print("Warning - No results file. Cannot continue...")
         return (1)

      resultsFileInfo = dataStoreFileInfoList[0][0]
      localResultsFile = downloadAndRenameResultsFile(options, resultsFileInfo, requestName, downloadURL = downloadURL)

      if not localResultsFile:
         print("Results file error. Cannot continue...")
         return (1)

      # The last list of dicts should be our image files
      imageInfo = dataStoreFileInfoList[-1]

      headers = pf.open(localResultsFile)
      table = headers[1].data

      # 2015-02-24 KWS No need to deal with CMF files anymore. Let's use
      #                the PS1 Ubercal Stars catalog.
      for fitsRow in table:
         if '.fits' in fitsRow.field('IMG_NAME'):

            if fitsRow.field('ERROR_CODE') == 0:
               dataStoreFileInfo = findDictInDictList(imageInfo, fitsRow.field('IMG_NAME'))
               if not dataStoreFileInfo:
                  print("WARNING: IMG_NAME '%s' is not in the index" % fitsRow.field('IMG_NAME'))
                  print("Writing dummy record.")
                  recordId = insertPostageStampImageRecord(conn, fitsRow.field('COMMENT') + 'finder', None, None, ERROR_FILE_NOT_IN_DATASTORE_INDEX, groupType=GROUP_TYPE_FINDER)
               else:
                  print("Downloading image...")
                  downloadStatus = downloadFinderImage2(conn, requestName, fitsRow, dataStoreFileInfo, PSSImageRootLocation, offsetStarFilter = offsetStarFilter, dataStoreURL = downloadURL, nsigma = nsigma, connSherlock = connSherlock)
                  if downloadStatus == True:
                     print("Downloaded the image successfully.")
                  else:
                     print("Download of the image failed.")

            else:
               # We have an error. Write a dummy record to the images table
               print("WARNING: The PSS could not produce the image. Error code = %d." % fitsRow.field('ERROR_CODE'))
               recordId = insertPostageStampImageRecord(conn, fitsRow.field('COMMENT') + 'finder', None, None, fitsRow.field('ERROR_CODE'), groupType=GROUP_TYPE_FINDER)


   return (0)




def downloadPostageStampResults(conn, options, psRequests, PSSImageRootLocation, offsetStarFilter = None, downloadURL = None, nsigma = 2.0, connSherlock = None):
   """downloadPostageStampResults.

   Args:
       conn:
       options:
       psRequests:
       PSSImageRootLocation:
       offsetStarFilter:
       downloadURL:
       connSherlock:
   """

   # Update the status to "downloading"
   for row in psRequests:
      requestName = row["name"]
      if not (updateRequestStatus(conn, requestName, DOWNLOADING) > 0):
         print("Problem updating the database.")


   # Set all urllib requests to timeout after 10 minutes, otherwise it will wait forever
   import socket
   socket.setdefaulttimeout(600)

   if psRequests:
      for row in psRequests:
         requestName = row["name"]
         print("Processing Request: %s" % requestName)
         if downloadAllFinderImages(conn, options, requestName, PSSImageRootLocation, offsetStarFilter = offsetStarFilter, downloadURL = downloadURL, nsigma = nsigma, connSherlock = connSherlock) == 0:
            # Update the request status
            if (updateRequestStatus(conn, requestName, COMPLETE) > 0):
               print("Successfully downloaded request from Postage Stamp Server and updated database.")
            else:
               print("Submitted job, but did not update database.")

         else:
            print("Could not process this request.")
            if not (updateRequestStatus(conn, requestName, COMMUNICATION_ERROR) > 0):
               print("Problem updating the database.")

   print("Processing complete.")
   return (0)


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

   username = config['databases']['local']['username']
   password = config['databases']['local']['password']
   database = config['databases']['local']['database']
   hostname = config['databases']['local']['hostname']

   susername = config['databases']['sherlock']['username']
   spassword = config['databases']['sherlock']['password']
   sdatabase = config['databases']['sherlock']['database']
   shostname = config['databases']['sherlock']['hostname']

   downloadURL = None
   try:
      downloadURL = config['postage_stamp_parameters']['downloadurl']
   except KeyError as e:
      print("Cannot find the download URL. Exiting.")
      sys.exit(1)

   conn = dbConnect(hostname, username, password, database)
   connSherlock = dbConnect(shostname, susername, spassword, sdatabase)

   # The Image Root Location will be dependent on the name of the database.
   PSSImageRootLocation = '/' + hostname + '/images/' + database

   if not os.path.exists(PSSImageRootLocation):
      os.makedirs(PSSImageRootLocation)
      os.chmod(PSSImageRootLocation, 0o775)

   limit = 5

   psRequests = getPSRequestList(conn, SUBMITTED, requestType = FINDER_REQUEST_V2)
   timedOutRequests = getPSRequestList(conn, TIMEOUT, requestType = FINDER_REQUEST_V2)
   communicationErrorRequests = getPSRequestList(conn, COMMUNICATION_ERROR, requestType = FINDER_REQUEST_V2)

   if timedOutRequests:
      psRequests += timedOutRequests

   if communicationErrorRequests:
      psRequests += communicationErrorRequests

   downloadPostageStampResults(conn, options, psRequests, PSSImageRootLocation, offsetStarFilter = 'r', downloadURL = downloadURL, nsigma = float(options.nsigma), connSherlock = connSherlock)
   conn.close()
   connSherlock.close()




if __name__=="__main__":
    main()
