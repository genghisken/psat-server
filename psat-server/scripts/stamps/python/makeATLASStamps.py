#!/usr/bin/env python
"""Make ATLAS Stamps in the context of the transient server database.

Usage:
  %s <configfile> [<candidate>...] [--detectionlist=<detectionlist>] [--customlist=<customlist>] [--limit=<limit>] [--earliest] [--nondetections] [--discoverylimit=<discoverylimit>] [--lastdetectionlimit=<lastdetectionlimit>] [--requesttype=<requesttype>] [--wpwarp=<wpwarp>] [--update] [--ddc] [--skipdownload] [--redregex=<redregex>] [--diffregex=<diffregex>] [--redlocation=<redlocation>] [--difflocation=<difflocation>]
  %s (-h | --help)
  %s --version

Options:
  -h --help                                   Show this screen.
  --version                                   Show version.
  --update                                    Update the database
  --detectionlist=<detectionlist>             List option
  --customlist=<customlist>                   Custom List option
  --limit=<limit>                             Number of detections for which to request images [default: 6]
  --earliest                                  By default, get the most recent stamps. Otherwise get the earliest ones.
  --nondetections                             Request non-detections.
  --discoverylimit=<discoverylimit>           Number of days before which we will not request images (ignored if non-detections not requested) [default: 10]
  --lastdetectionlimit=<lastdetectionlimit>   Number of days after the last detection we will request images (ignored if non-detections not requested) [default: 20]
  --requesttype=<requesttype>                 Request type (all | incremental) [default: incremental]
  --ddc                                       Use the DDC schema for queries
  --skipdownload                              Do not attempt to download the exposures (assumes they already exist locally)
  --wpwarp=<wpwarp>                           Which version of wpwarp to use? (1 or 2) [default: 2]
  --redregex=<redregex>                       Reduced image regular expression. Caps = variable. [default: EXPNAME.fits.fz]
  --diffregex=<diffregex>                     Diff image regular expression. Caps = variable. [default: EXPNAME.diff.fz]
  --redlocation=<redlocation>                 Reduced image location. E.g. /atlas/diff/CAMERA/fake/MJD.fake (caps = special variable).  Null value means use standard ATLAS archive location.
  --difflocation=<difflocation>               Diff image location. E.g. /atlas/diff/CAMERA/fake/MJD.fake (caps = special variable). Null value means use standard ATLAS archive location.

E.g.:
  %s ~/config_fakers.yaml 1130252001002421600 --ddc --skipdownload --redlocation=/atlas/diff/CAMERA/fake/MJD.fake --redregex=EXPNAME.fits+fake --difflocation=/atlas/diff/CAMERA/fake/MJD.fake --diffregex=EXPNAME.diff+fake
"""

import sys
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt
import os, shutil, re, csv, subprocess

# In this module, we want to split out the rsync stuff from the stamp production.
# The reason for this is that we want to be able to call this code from a multiprocessing
# module, which will multiprocess the rsyncs differently from the stamp production.
# We'll need to rewrite the code below so that we can do the split.

# This time, we'll do the rsync calculation at the beginning of the code for BOTH
# diff and target images.

# So the sequence will be:

# 1. Calculate unique exposures.
# 2. Multiprocessing: Kick off up to 10 rsync threads to GET the exposures. To do this
#    we need to split out the rsync code into a worker method.
# 3. When rsync complete, split the objects into nCPU threads and generate stamps

# Notes: 1. The code should be flexible enough to use EITHER getfits or monsta.
#        2. The code should be flexible enough to use EITHER internal generation of jpegs OR monsta generated jpegs.

# If generating jpegs with monsta, need to specify what max and min are.  (Maybe hard wired, as in some of the monsta
# code.

# 2015-12-02 KWS Added new version of this code.

import sys, os, errno
import datetime
import subprocess
from gkutils.commonutils import dbConnect, PROCESSING_FLAGS, calculateRMSScatter, Struct, cleanOptions
import MySQLdb
from pstamp_utils import getLightcurveDetectionsAtlas2, getExistingDetectionImages, getExistingNonDetectionImages, DETECTIONTYPES, REQUESTTYPES, PSTAMP_SUCCESS, PSTAMP_NO_OVERLAP, PSTAMP_SYSTEM_ERROR, IPP_IDET_NON_DETECTION_VALUE, insertPostageStampImageRecordAtlas
import image_utils as imu
#import pyfits as pf
from astropy.io import fits as pf
from psat_server_web.atlas.atlas.commonqueries import getLightcurvePoints, getNonDetections, getNonDetectionsUsingATLASFootprint, ATLAS_METADATADDC, filterWhereClauseddc, LC_POINTS_QUERY_ATLAS_DDC, FILTERS
from random import shuffle

def updateAtlasObjectProcessingFlag(conn, candidate, processingFlag = PROCESSING_FLAGS['stamps']):
    """Update the processing flag for the relevant database object to prevent repeat processing of the same objects.

    Args:
        conn:
        candidate:
        processingFlag:
    """

    import MySQLdb

    updatedRows = 0

    try:
        cursor = conn.cursor (MySQLdb.cursors.DictCursor)

        cursor.execute ("""
            update atlas_diff_objects
            set processing_flags = if(processing_flags is null, %s, processing_flags | %s)
            where id = %s
            """, (processingFlag, processingFlag, candidate['id']))

        updatedRows = cursor.rowcount
        cursor.close()

    except MySQLdb.Error as e:
        sys.stderr.write("Error %d: %s\n" % (e.args[0], e.args[1]))

    return updatedRows


def eliminateExistingImages(conn, candidate, detections, detectionsWithImages):
   """eliminateExistingImages.

   Args:
       conn:
       candidate:
       detections:
       detectionsWithImages:
   """

   imagesToRequest = []

   for row in detections:

      if '%d_%s_%s_%d' % (candidate, row.tdate, row.expname, row.tphot_id) not in detectionsWithImages:
         imagesToRequest.append(row)

   return imagesToRequest



# imageType = 'diff' or 'red'
#def doRsync(exposureSet, imageType, userId = 'xfer', remoteMachine = 'atlas-base-adm02.ifa.hawaii.edu', remoteLocation = '/atlas', localLocation = '/atlas', getMetadata = False, metadataExtension = '.tph'):
def doRsync(exposureSet, imageType, userId = 'xfer', remoteMachine = 'atlas-base-adm02.ifa.hawaii.edu', remoteLocation = '/atlas', localLocation = '/atlas', getMetadata = False, metadataExtension = '.tph'):
    """doRsync.

    Args:
        exposureSet:
        imageType:
        userId:
        remoteMachine:
        remoteLocation:
        localLocation:
        getMetadata:
        metadataExtension:
    """

    exposureSet.sort()
    rsyncCmd = '/usr/bin/rsync'

    if imageType not in ['diff','red']:
        print("Image type must be diff or red")
        return 1

    imageExtension = {'diff':'.diff.fz','red':'.fits.fz'}

    rsyncFile = '/tmp/rsyncFiles_' + imageType + str(os.getpid()) + '.txt'

    # Create a diff and input rsync file
    rsf = open(rsyncFile, 'w')
    for exp in exposureSet:
        camera = exp[0:3]
        mjd = exp[3:8]

#        # 2017-01-20 KWS Reprocessing of 'red' images taking place. Old data is in 02a.ORIG.
#        if imageType == 'red' and camera == '02a' and int(mjd) <= 57771:
#            camera = '02a.ORIG'
        imageName = camera + '/' + mjd + '/' + exp + imageExtension[imageType]

        if getMetadata:
            # We don't need the image, just get the metadata
            imageName = camera + '/' + mjd + '/' + exp + metadataExtension
            if metadataExtension == '.tph' and int(mjd) >= 57350:
                imageName = camera + '/' + mjd + '/' + 'AUX/' + exp + metadataExtension

        rsf.write('%s\n' % imageName)

    rsf.close()

    remote = userId + '@' + remoteMachine + ':' + remoteLocation + '/' + imageType
    local = localLocation + '/' + imageType

    # Get the diff images
    # 2018-04-16 KWS Removed the 'u' flag. We don't need to update the images.
    #p = subprocess.Popen([rsyncCmd, '-e "ssh -c arcfour -o Compression=no"', '-axKL', '--files-from=%s' % rsyncFile, remote, local], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # 2022-07-21 KWS Added text=True to the Popen command. Ensures that the response comes back as text.
    p = subprocess.Popen([rsyncCmd, '-avxKL', '--files-from=%s' % rsyncFile, remote, local], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    output, errors = p.communicate()

    if output.strip():
        print(output)
    if errors.strip():
        print(errors)

    return 0


# 2016-10-10 KWS I seem to be calling the same code exactly more than once, so lets just use a single function
def getLightcurveData(conn, candidate, limit = 0, mostRecent = True, nonDets = False, discoveryLimit = 30, lastDetectionLimit=20, requestType = REQUESTTYPES['incremental'], ddc = False):
    """getLightcurveData.

    Args:
        conn:
        candidate:
        limit:
        mostRecent:
        nonDets:
        discoveryLimit:
        lastDetectionLimit:
        requestType:
        ddc:
    """

    #lightcurveData = getLightcurveDetectionsAtlas2(conn, candidate['id'], limit = limit, mostRecent = mostRecent)
    if ddc:
        p, recurrences = getLightcurvePoints(candidate['id'], lcQuery=LC_POINTS_QUERY_ATLAS_DDC + filterWhereClauseddc(FILTERS), conn = conn)
    else:
        p, recurrences = getLightcurvePoints(candidate['id'], conn = conn)

    existingImages = getExistingDetectionImages(conn, candidate['id'])
    firstDetection = recurrences[0]
    lastDetection = recurrences[-1]
    if mostRecent:
        # reverse the order of the list
        recurrences.reverse()

    # Get the mean RA and Dec.
    objectCoords = []
    for row in recurrences:
        objectCoords.append({'RA': row.ra, 'DEC': row.dec})
    avgRa, avgDec, rms = calculateRMSScatter(objectCoords)

    if limit:
        if len(recurrences) > limit:
            recurrences = recurrences[0:limit]

        # 2018-11-03 KWS Don't bother requesting images more than 10 days
        #                older than the most recent recurrence. This will stop
        #                the rsyncing of old data.
        # 2018-11-05 KWS Don't bother requesting images where row.dup < 0.
        if mostRecent:
            recentMJD = recurrences[0].mjd
            truncatedRecurrences = []
            for row in recurrences:
                if row.dup >= 0 and row.mjd > recentMJD - 50:
                    truncatedRecurrences.append(row)
            if len(truncatedRecurrences) > 0:
                recurrences = truncatedRecurrences

    # But only go back as far as firstDetection - discoveryLimit
    if nonDets: # and not limit:
        #b, blanks, lastNonDetection = getNonDetections(recurrences, conn = conn, searchRadius=500, tolerance = 0.001)
        if ddc:
            b, blanks, lastNonDetection = getNonDetectionsUsingATLASFootprint(recurrences, conn = conn, ndQuery=ATLAS_METADATADDC, filterWhereClause = filterWhereClauseddc, catalogueName = 'atlas_metadataddc')
        else:
            b, blanks, lastNonDetection = getNonDetectionsUsingATLASFootprint(recurrences, conn = conn)
        existingImages += getExistingNonDetectionImages(conn, candidate['id'])
        for row in blanks:
            if row.mjd >= firstDetection.mjd - discoveryLimit and row.mjd <= lastDetection.mjd + lastDetectionLimit:
                row.tphot_id = IPP_IDET_NON_DETECTION_VALUE
                recurrences.append(row)

    if requestType == REQUESTTYPES['incremental']:
        recurrences = eliminateExistingImages(conn, candidate['id'], recurrences, existingImages)

    return recurrences, avgRa, avgDec

# Function to find the unique exposures, given a candidate list.

# 2016-10-06 KWS Get the non-detections as well
def getUniqueExposures(conn, candidateList, limit = 0, mostRecent = True, nonDets = False, discoveryLimit = 10, lastDetectionLimit=20, requestType = REQUESTTYPES['incremental'], ddc = False):
    """getUniqueExposures.

    Args:
        conn:
        candidateList:
        limit:
        mostRecent:
        nonDets:
        discoveryLimit:
        lastDetectionLimit:
        requestType:
        ddc:
    """
    print("Finding Unique Exposures...")
    exposures = []

    # Always get all of the detection exposures
    for candidate in candidateList:
        recurrences, avgRa, avgDec = getLightcurveData(conn, candidate, limit = limit, mostRecent = mostRecent, nonDets = nonDets, discoveryLimit = discoveryLimit, lastDetectionLimit=lastDetectionLimit, requestType = requestType, ddc = ddc)
        for row in recurrences:
            exposures.append(row.expname)

    exposureSet = list(set(exposures))
    # 2016-10-07 KWS The problem is that the most recent exposures are probably
    #                the ones we need to collect.  But this almost certainly means
    #                that only one thread will end up downloading the data if the
    #                data is sorted.  So shuffle it.
    shuffle(exposureSet)

    return exposureSet


def downloadExposures(exposureSet, useMonsta = True):
   """downloadExposures.

   Args:
       exposureSet:
       useMonsta:
   """

   funpackCmd = '/atlas/bin/funpack'

   # (1.1) Get the diff images.  We no longer download these by default.

   print("Fetching Diff Images...")
   doRsync(exposureSet, 'diff')

   # (3) Go and get the input exposures

   print("Fetching Input Images...")
   doRsync(exposureSet, 'red')

   # (2) Unpack the diff data (which we already have) to a temporary location

   if not useMonsta:
       print("Unpacking Diff Images...")
       for file in exposureSet:
          camera = file[0:3]
          mjd = file[3:8]
    
          originalExposure = camera + '/' + mjd + '/' + file + '.fits.fz'
          rsf.write('%s\n' % originalExposure)
    
          inputFilename = '/atlas/diff/' + camera + '/' + mjd + '/' + file + '.diff.fz'
    
          outputFileDirectory = '/atlas/unpacked/diff/' + camera + '/' + mjd + '/'
    
          # Create the directory if it doesn't exist
          if not os.path.exists(outputFileDirectory):
             try:
                os.makedirs(outputFileDirectory)
             except OSError as e:
                if e.errno == errno.EEXIST and os.path.isdir(outputFileDirectory):
                   pass
                else:
                   raise
             os.chmod(outputFileDirectory, 0o775)
    
          outputFilename = outputFileDirectory + file + '.diff'
    
          p = subprocess.Popen([funpackCmd, '-O', outputFilename, inputFilename], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
          output, errors = p.communicate()
    
          if output.strip():
             print(output)
          if errors.strip():
             print(errors)



   # (4) Now that we have the exposures, unpack them - same as above - should extract into a function


   if not useMonsta:
       print("Unpacking Input Images...")
       for file in exposureSet:
          camera = file[0:3]
          mjd = file[3:8]
    
          inputFilename = '/atlas/red/' + camera + '/' + mjd + '/' + file + '.fits.fz'
    
          outputFileDirectory = '/atlas/unpacked/red/' + camera + '/' + mjd + '/'
    
          # Create the directory if it doesn't exist
          if not os.path.exists(outputFileDirectory):
             try:
                os.makedirs(outputFileDirectory)
             except OSError as e:
                if e.errno == errno.EEXIST and os.path.isdir(outputFileDirectory):
                   pass
                else:
                   raise
             os.chmod(outputFileDirectory, 0o775)
    
          outputFilename = outputFileDirectory + file + '.fits'
    
          p = subprocess.Popen([funpackCmd, '-O', outputFilename, inputFilename], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
          output, errors = p.communicate()
    
          if output.strip():
             print(output)
          if errors.strip():
             print(errors)
    
   return exposureSet


# 2015-12-02 KWS New version of this code for the ATLAS ddet schema
# 2016-10-10 KWS Added ability to request non-detections
def makeATLASObjectPostageStamps3(conn, candidateList, PSSImageRootLocation, stampSize = 200, limit = 0, mostRecent = True, detectionType = DETECTIONTYPES['detections'], requestType = REQUESTTYPES['incremental'], useMonsta = True, nonDets = False, discoveryLimit = 10, lastDetectionLimit=20, ddc = False, wpwarp = 1, remoteLocation = 'xfer@atlas-base-adm02.ifa.hawaii.edu:/atlas/red', remoteDiffLocation = 'xfer@atlas-base-adm02.ifa.hawaii.edu:/atlas/diff', localLocation = '/atlas/red', localDiffLocation = '/atlas/diff', options = None):
   """makeATLASObjectPostageStamps3.

   Args:
       conn:
       candidateList:
       PSSImageRootLocation:
       stampSize:
       limit:
       mostRecent:
       detectionType:
       requestType:
       useMonsta:
       nonDets:
       discoveryLimit:
       lastDetectionLimit:
       ddc:
       wpwarp:
       remoteLocation:
       remoteDiffLocation:
       localLocation:
       localDiffLocation:
       options:
   """


   import subprocess
   from collections import OrderedDict


   unpackedDiffLocation='/atlas/unpacked/'
   #remoteLocation = 'xfer@atlas-base-adm02.ifa.hawaii.edu:/atlas/red'
   #remoteDiffLocation = 'xfer@atlas-base-adm02.ifa.hawaii.edu:/atlas/diff'
   #localLocation = '/atlas/red'
   #localDiffLocation = '/atlas/diff'
   
   wpwarp1Cmd = '/atlas/bin/wpwarp1'
   wpwarp2Cmd = '/atlas/bin/wpwarp2'
   monstaCmd = '/atlas/vendor/monsta/bin/monsta'
   monstaScript = '/atlas/lib/monsta/subarray.pro'

   pix2skyCmd = '/atlas/bin/pix2sky'

   # New code needs to do this:

   # Gather together all the unique diff exposures, then unpack them to a temporary location.
   # Rsync the associated /atlas/red exposures from Hawaii.  Unpack them to a temporary location

   # Example rsync command: rsync -auvx \
   #                              --files-from=/tmp/rsync_file_list.txt \
   #                              xfer@atlas-base-adm02.ifa.hawaii.edu:/atlas/red/02a /tmp/kws_rsync_test
   #
   # where /tmp/rsync_file_list.txt contains (e.g.):
   #
   # 57334/02a57334o0024c.fits.fz
   # 57334/02a57334o0020c.fits.fz
   # 57334/02a57334o0021c.fits.fz
   #

   # To do the above, we will need to sweep through all objects and all points in lightcurve,
   # creating a set of expname.

   # (optionally) Generate wallpaper templates.  (Could be done on a per-stamp basis.)
   # (Could create an /atlas/unpacked/red, /atlas/unpacked/diff and /atlas/unpacked/tmpl directories.


   # (5) Use Getfits to create substamps and use wpwarp1 to generate templates from the substamps.
    
   dx = dy = stampSize

   currentDate = datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")
   (year, month, day, hour, min, sec) = currentDate.split(':')

   objectsModified = set()

   for candidate in candidateList:
      imageRequestData = []
      print()
      print("***************************************")
      print("**** Candidate %d ****" % candidate['id'])
      print("***************************************")
      print()

      if detectionType == DETECTIONTYPES['detections'] and limit == 0:
         print(candidate)
         recurrences, avgRa, avgDec = getLightcurveData(conn, candidate, limit = limit, mostRecent = mostRecent, nonDets = nonDets, discoveryLimit = discoveryLimit, lastDetectionLimit=lastDetectionLimit, requestType = requestType, ddc = ddc)
      else:
         # Assume a limit is set, otherwise no request will get sent. Setting a limit
         # forces only detections to be requested and this will always force the most
         # most recent <limit> images to be requested.
         if limit > 0:
            recurrences, avgRa, avgDec = getLightcurveData(conn, candidate, limit = limit, mostRecent = mostRecent, nonDets = nonDets, discoveryLimit = discoveryLimit, lastDetectionLimit=lastDetectionLimit, requestType = requestType, ddc = ddc)


         # We need to process a triplet of images at once.

      for row in recurrences:
         x = None
         y = None

         (objectId, tdate, diffId, ippIdet) = (candidate['id'], row.tdate, row.expname, row.tphot_id)

         camera = diffId[0:3]
         mjd = diffId[3:8]
         
         imageGroupName = "%d_%s_%s_%s" % (objectId, tdate, diffId, ippIdet)


         if useMonsta:
            if options is not None and options.redregex is not None and options.redlocation is not None and options.diffregex is not None and options.difflocation is not None: 
               targetImage = options.redlocation.replace('CAMERA', camera).replace('MJD', mjd) + '/' + options.redregex.replace('EXPNAME', diffId)
               diffImage = options.difflocation.replace('CAMERA', camera).replace('MJD', mjd) + '/' + options.diffregex.replace('EXPNAME', diffId)
            else:
               targetImage = localLocation + '/' + camera + '/' + mjd + '/' + diffId + '.fits.fz'
  #             if camera == '02a' and int(mjd) <= 57771:
  #                 targetImage = '/atlas/red/' + camera + '.ORIG' + '/' + mjd + '/' + diffId + '.fits.fz'
               diffImage = localDiffLocation + '/' + camera + '/' + mjd + '/' + diffId + '.diff.fz'
         else:
             targetImage = '/atlas/unpacked/red/' + camera + '/' + mjd + '/' + diffId + '.fits'
#             if camera == '02a' and int(mjd) <= 57771:
#                 targetImage = '/atlas/unpacked/red/' + camera + '.ORIG' + '/' + mjd + '/' + diffId + '.fits'
             diffImage = '/atlas/unpacked/diff/' + camera + '/' + mjd + '/' + diffId + '.diff'

         if ippIdet == IPP_IDET_NON_DETECTION_VALUE:
            xhColor = 'brown1'
            p = subprocess.Popen([pix2skyCmd, '-sky2pix', diffImage, str(avgRa), str(avgDec)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            output, errors = p.communicate()
            if output:
                x,y = output.split()
                try:
                   x = float(x)
                   y = float(y)
                except ValueError as e:
                   print("Unable to convert x and y to float.")
                   continue
                if x < 0 or x > 10560 or y < 0 or y > 10560:
                   print("x or y out of bounds (%f, %f)" % (x, y))
         else:
            xhColor = 'green1'
            x = row.x
            y = row.y
            if useMonsta:
               x += 0.5
            else:
               x -= 0.5

         if x is None or y is None:
            # Can't continue: skip stamp creation
            print("Required image doesn't exist!")
            continue

         print(candidate['id'], row.tdate, row.expname, row.tphot_id, avgRa, avgDec, x, y)

         # We need to generate the template image using wpwarp1
         refImage = diffId + '.tmpl'

         imageFilenames = {'diff': diffImage, 'ref': refImage, 'target': targetImage}

         # 2017-01-29 KWS Bug in wpwarp1 means that new diff images written with two END
         #                cards. Apparently the reduced image does open OK, so use IT
         #                to generate the reference stamp rather than the diff. But we
         #                need to change the order of the dictionary so make sure that
         #                that the reduced image stamp exists first - hence use of
         #                OrderedDict.
         imageFilenames = OrderedDict(sorted(list(imageFilenames.items()), reverse=True))

         diffFilenameDirectory = os.path.dirname(imageFilenames['diff'])

         imageMJD = None
         imageFilterId = None

         for imageType, imageName in imageFilenames.items():
            # 1. Get image stamp
            # 2. Save the stamp in the right place
            # 3. Generate a jpeg from the stamp and locate in same place as stamp
            # 4. Insert a database record in tcs_postage_stamp_images and tcs_image_groups

            outputFilename = imageGroupName + '_' + imageType + '.fits'

            errorCode = PSTAMP_SUCCESS
            # If too near to the edge set the error code to something other than zero

            # ImageMJD must be extracted from the image data.

            maskedPixelRatio = None
            maskedPixelRatioAtCore = None

            localImageName = imageGroupName + '_' + imageType
            pssImageName = imageFilenames[imageType].split('/')[-1]

            flip = False

            imageDownloadLocation = PSSImageRootLocation + '/' + mjd

            # Create the relevant MJD directory under the images root
            # 2015-04-01 KWS Sometimes the path doesn't exist at the point of test, but then
            #                does exist when makedirs is called (e.g. created by another process).
            #                Hence insert new try block here.
            if not os.path.exists(imageDownloadLocation):
               try:
                  os.makedirs(imageDownloadLocation)
               except OSError as e:
                  if e.errno == errno.EEXIST and os.path.isdir(imageDownloadLocation):
                     pass
                  else:
                     raise
               os.chmod(imageDownloadLocation, 0o775)

            # Download the image and rename as COMMENT (if we haven't aready downloaded it)
            absoluteLocalImageName = imageDownloadLocation + '/' + localImageName + '.fits'

            if imageType == 'ref':
               print("Creating Template Image...")
               # Use wpwarp1 to create a template stamp. Since the diff image should have been
               # created first, then we should be able to use this to create the template.
               #diffImage = imageDownloadLocation + '/' + imageGroupName + '_' + 'diff.fits'
               # 2017-01-29 KWS Bug in wpwarp1 means that new diff images written with two END
               #                cards. It seems that pyfits can't cope with this, so use the
               #                target image to define the reference, not the diff image.
               #                Note that the target image stamp also has two END cards. Why
               #                this works and diff images don't is anyone's guess!
               targetImage = imageDownloadLocation + '/' + imageGroupName + '_' + 'target.fits'
               #p = subprocess.Popen([wpwarp1Cmd, '-samp', absoluteLocalImageName, diffImage], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
               if wpwarp == 1:
                   p = subprocess.Popen([wpwarp1Cmd, '-samp', absoluteLocalImageName, targetImage], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
               else:
                   # Use the new wpwarp2. E.g. wpwarp2 -novar -nomask -nozerosat -wp /tmp/02a58993o0492c_new.tmpl /tmp/ATLAS20nvd_02a58993o0492c.fits
                   print(wpwarp2Cmd, '-novar', '-nomask', '-nozerosat', '-wp', absoluteLocalImageName, targetImage)
                   p = subprocess.Popen([wpwarp2Cmd, '-novar', '-nomask', '-nozerosat', '-wp', absoluteLocalImageName, targetImage], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

               output, errors = p.communicate()
               rc = p.returncode
               print("Return Code =", rc)
               if not rc:
                   status = PSTAMP_SUCCESS
               else:
                   status = PSTAMP_SYSTEM_ERROR

            else:
               # Cut out the stamp
               print("Snipping Stamp Image...")
               rot = 0
               if useMonsta:
                   #status, rot = imu.getMonstaPostageStamp(imageFilenames[imageType], absoluteLocalImageName, (row['x'] - 0.5), (row['y'] - 0.5), dx)
                   status, rot = imu.getMonstaPostageStamp(imageFilenames[imageType], absoluteLocalImageName, x, y, dx)
               else:
                   status = imu.getFITSPostageStamp(imageFilenames[imageType], absoluteLocalImageName, x, y, dx, dy)

            if status == PSTAMP_SUCCESS:
               print("Got stamp...")
               # Add the object to the set of objects that were updated.

               objectsModified.add(objectId)

               try:
                   hdus = pf.open(absoluteLocalImageName)
               except IOError as e:
                   print(e)
                   print("Cannot open file %s! Abandon this image for the time being." % absoluteLocalImageName)
                   continue

               header = []
               try:
                  header = hdus[1].header
               
               except IndexError as e:
                  print("This looks unpacked.  Try opening it as unpacked...")
                  header = hdus[0].header


               try:
                  #imageFilterId = photcodeMap[header['PHOTCODE'][-4:]] # last 4 characters
                  imageFilterId = header['FILTER'] # last 4 characters
                  imageMJD = header['MJD-OBS']
               except KeyError as e:
                  print("Vital header is missing from file %s.  Cannot continue." % absoluteLocalImageName)
                  print(e)
                  # wpwarp1 currently messes up the header. Use the previous value of imageFilterId and imageMJD
                  # for the time being.
                  #return 1

               hdus.close()

               # Convert to JPEG & add frame & crosshairs - write image
               # 2011-07-23 KWS Need to disable flipping for V3 images.
               # 2014-06-24 KWS Change quality to 100 for ATLAS images.
               (maskedPixelRatio, maskedPixelRatioAtCore) = imu.convertFitsToJpegWithCrosshairs2(absoluteLocalImageName, imu.fitsToJpegExtension(absoluteLocalImageName), flip = flip, xhColor = xhColor, nsigma = 2, quality = 100, magicNumber = -31415, rotate = rot)

               # Create an image record using IMG_NAME and COMMENT
               # This call also makes the association of the the image group with the
               # object in tcs_transient_objects and tcs_transient_reobservations.
               (imageId, imageGroupId) = insertPostageStampImageRecordAtlas(conn, localImageName, pssImageName, imageMJD, errorCode, imageFilterId, maskedPixelRatio, maskedPixelRatioAtCore, ddc = ddc)
            else:
               (imageId, imageGroupId) = insertPostageStampImageRecordAtlas(conn, localImageName, None, imageMJD, status, imageFilterId, maskedPixelRatio, maskedPixelRatioAtCore, ddc = ddc)

      # Set the 'stamps' processing flag so we can avoid processing this object more than once.
      updateAtlasObjectProcessingFlag(conn, candidate)

   # 2013-07-04 KWS We'd like to update the tcs_images table with the most recent image triplet.
#   if objectsModified:
#      updateTriplets(conn, objectsModified)

   return candidateList


def getObjectsByList(conn, listId = 4, objectType = -1, dateThreshold = '2009-01-01', processingFlags = PROCESSING_FLAGS['stamps']):
    """getObjectsByList.

    Args:
        conn:
        listId:
        objectType:
        dateThreshold:
        processingFlags:
    """

    try:
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)

        cursor.execute ("""
            select id, ra, `dec`, id 'name', followup_flag_date, atlas_designation, other_designation, zooniverse_score, realbogus_factor
            from atlas_diff_objects
            where detection_list_id = %s
            and (object_classification is null or object_classification & %s > 0)
            and (processing_flags & %s = 0 or processing_flags is null)
            and followup_flag_date > %s
            and sherlockClassification is not null
            and sherlockClassification not in ('VS','BS')
            order by followup_id
        """, (listId, objectType, processingFlags, dateThreshold))

        resultSet = cursor.fetchall ()
        cursor.close ()

    except MySQLdb.Error as e:
        print("Error %d: %s" % (e.args[0], e.args[1]))
        sys.exit (1)

    return resultSet


def getObjectsByCustomList(conn, customList, objectType = -1, processingFlags = PROCESSING_FLAGS['stamps']):
    """getObjectsByCustomList.

    Args:
        conn:
        customList:
        objectType:
        processingFlags:
    """

    try:
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute ("""
            select o.id, o.ra, o.`dec`, o.id 'name', o.followup_flag_date, o.atlas_designation, o.zooniverse_score, o.realbogus_factor
            from atlas_diff_objects o, tcs_object_groups g
            where g.object_group_id = %s
              and g.transient_object_id = o.id
              and (processing_flags & %s = 0 or processing_flags is null)
            order by o.followup_id
        """, (customList, processingFlags))

        resultSet = cursor.fetchall ()
        cursor.close ()

    except MySQLdb.Error as e:
        print("Error %d: %s" % (e.args[0], e.args[1]))
        sys.exit (1)

    return resultSet


# ###########################################################################################
#                                         Main program
# ###########################################################################################

def main():
    """main.
    """
    opts = docopt(__doc__, version='0.1')
    opts = cleanOptions(opts)
    options = Struct(**opts)

    configFile = options.configfile

    import yaml
    with open(configFile) as yaml_file:
        config = yaml.load(yaml_file)

    username = config['databases']['local']['username']
    password = config['databases']['local']['password']
    database = config['databases']['local']['database']
    hostname = config['databases']['local']['hostname']

    detectionList = 1
    customList = None

    conn = dbConnect(hostname, username, password, database)
    # 2023-03-13 KWS MySQLdb disables autocommit by default. Switch it on globally.
    conn.autocommit(True)


    update = options.update
    limit = int(options.limit)
    mostRecent = not(options.earliest)
    nondetections = options.nondetections
    discoverylimit = int(options.discoverylimit)
    lastdetectionlimit = int(options.lastdetectionlimit)

    try:
        requestType = REQUESTTYPES[options.requesttype]
    except KeyError as e:
        requestType = REQUESTTYPES['incremental']

    objectList = []

    if options.candidate is not None and len(options.candidate) > 0:
        for cand in options.candidate:
            objectList.append({'id': int(cand)})
    else:

        if options.customlist is not None:
            if int(options.customlist) > 0 and int(options.customlist) < 100:
                customList = int(options.customlist)
                objectList = getObjectsByCustomList(conn, customList)
            else:
                print("The list must be between 1 and 100 inclusive.  Exiting.")
                sys.exit(1)
        else:
            if options.detectionlist is not None:
                if int(options.detectionlist) >= 0 and int(options.detectionlist) < 9:
                    detectionList = int(options.detectionlist)
                    objectList = getObjectsByList(conn, listId = detectionList)
                else:
                    print("The list must be between 0 and 6 inclusive.  Exiting.")
                    sys.exit(1)

    print("LENGTH OF OBJECTLIST = ", len(objectList))

    PSSImageRootLocation = '/' + hostname + '/images/' + database

    #exposureSet = getUniqueExposures(conn, objectList, limit = limit, mostRecent = mostRecent)
    # Only download exposures if requested. Otherwise assume we already HAVE the data.
    if not options.skipdownload:
        exposureSet = getUniqueExposures(conn, objectList, limit = limit, mostRecent = mostRecent, nonDets = nondetections, discoveryLimit = discoverylimit, lastDetectionLimit=lastdetectionlimit, ddc = options.ddc)
        exposureSet.sort()
        for row in exposureSet:
            print(row)
        downloadExposures(exposureSet)

    makeATLASObjectPostageStamps3(conn, objectList, PSSImageRootLocation, limit = limit, mostRecent = mostRecent, nonDets = nondetections, discoveryLimit = discoverylimit, lastDetectionLimit=lastdetectionlimit, requestType = requestType, ddc = options.ddc, wpwarp = options.wpwarp, options = options)

    conn.close()

if __name__ == '__main__':
    main()
