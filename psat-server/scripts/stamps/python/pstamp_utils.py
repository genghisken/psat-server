# I know one can do these imports inline, but I like them at the top to show
# what the clear dependencies are. All of these are reasonably easy to setup
# if they're not already part of python.  The exceptions are MultipartPostHandler
# and image_utils which need to be shipped with this code.

# 2013-08-08 KWS Moved the imports of MySQLdb into the database access code. This
#                allows this file to be used on machines that don't need database
#                access.
import urllib.request, urllib.error, urllib.parse
import urllib.request, urllib.parse, urllib.error
import hashlib
from astropy.io import fits as pf
import os,sys,errno
import image_utils as imu
import MultipartPostHandler
import http.cookiejar
import time
import datetime
from gkutils.commonutils import base26, PROCESSING_FLAGS, readPhotpipeDCMPFile, CAT_ID_RA_DEC_COLS
import re



width = 300
height = 300

requestHome = '/' + os.uname()[1].split('.')[0] + '/ingest/pstamp/requests'
if not os.path.exists(requestHome):
   requestHome = "/tmp"

# IMAGEID (which is an int) null value of -9999 means that it's not in the CMF file.
# We need to take care of this situation (i.e. skip requesting of diff ID if value is -9999
# or NULL).
IMAGEID_NULL_VALUE = -9999

# Constants describing the postage stamp request status - need to move to Constants class
CREATED             = 0
SUBMITTED           = 1
COMPLETE            = 2
COMMUNICATION_ERROR = 3
TIMEOUT             = 4
DOWNLOADING         = 5
CORRUPT             = 7

# Constants for acquiring Data Store Index pages.
OK                 = 0
PAGE_NOT_FOUND     = 1
BAD_SERVER_ADDRESS = 2
HTTP_ERROR         = 3

# Locally defined error codes for image download
ERROR_FILE_NOT_IN_DATASTORE_INDEX = -1
ERROR_BAD_FILE_CHECKSUM           = -2
ERROR_COULD_NOT_DOWNLOAD          = -3

# Postage stamp request types
POSTAGE_STAMP_REQUEST             = 0
DETECTABILITY_REQUEST             = 1
FULL_IMAGE_DATA_REQUEST           = 2
FINDER_REQUEST                    = 3
FINDER_REQUEST_V2                 = 4

# 2013-09-27 KWS Added postage stamp error codes
PSTAMP_SUCCESS         =  0 #No errors
PSTAMP_SYSTEM_ERROR    = 10 #Some unspecified system error occurred during processing
PSTAMP_NOT_IMPLEMENTED = 11 #Feature not yet implemented
PSTAMP_UNKNOWN_ERROR   = 12 #Unknown error
PSTAMP_DUP_REQUEST     = 20 #Request name is a duplicate
PSTAMP_INVALID_REQUEST = 21 #Error in request specification. See parse_error.txt
PSTAMP_UNKNOWN_PRODUCT = 22 #Unknown product in request specification
PSTAMP_NO_IMAGE_MATCH  = 23 #No images matched the request
PSTAMP_NOT_DESTREAKED  = 24 #Image matched, but not yet destreaked
PSTAMP_NOT_AVAILABLE   = 25 #Image not available (temporary)
PSTAMP_GONE            = 26 #Image is no longer available
PSTAMP_NO_JOBS_QUEUED  = 27 #Request specification yielded no jobs. See parse_error.txt
PSTAMP_NO_OVERLAP      = 28 #Center not contained in any image of interest
PSTAMP_NOT_AUTHORIZED  = 29 #
PSTAMP_NO_VALID_PIXELS = 30 #Stamps from skycells do not contain any finite pixels

# 2014-07-01 KWS Added custom stamp error codes
PSTAMP_EDGE_TOO_CLOSE  = 70


# IPP_IDET is never > 2147483648 (2 ** 31) in the database, and can never be > 4294967296 (2 ** 32)
# Therefore set ipp_idet for non-detections = 4300000000
IPP_IDET_NON_DETECTION_VALUE = 4300000000

# 2014-03-10 KWS Added new Group Type for Finder images
GROUP_TYPE_FINDER = 1

# MD5 sum program - stolen from the interweb...
# def md5(fileName, excludeLine="", includeLine=""):
#    """Compute md5 hash of the specified file"""
#    m = hashlib.md5()
#    try:
#       fd = open(fileName,"rb")
#    except IOError:
#       print("Unable to open the file in readmode:", fileName)
#       return
#    content = fd.readlines()
#    fd.close()
#    for eachLine in content:
#       if excludeLine and eachLine.startswith(excludeLine):
#          continue
#       m.update(eachLine)
#    m.update(includeLine)
#    return m.hexdigest()

def md5(filename, blocksize=65536):
    hash = hashlib.md5()
    with open(filename, "rb") as f:
        for block in iter(lambda: f.read(blocksize), b""):
            hash.update(block)
    return hash.hexdigest()


def dbConnect(lhost, luser, lpasswd, ldb, quitOnError=True):
   """dbConnect.

   Args:
       lhost:
       luser:
       lpasswd:
       ldb:
       quitOnError:
   """
   import MySQLdb
   try:
      conn = MySQLdb.connect (host = lhost,
                              user = luser,
                            passwd = lpasswd,
                                db = ldb)
   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))
      if quitOnError:
         sys.exit (1)
      else:
         conn=None

   return conn


def updateDownloadAttempts(conn, requestName, status):
   """updateDownloadAttempts.

   Args:
       conn:
       requestName:
       status:
   """
   import MySQLdb
   rowsUpdated = 0
   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      cursor.execute ("""
            update tcs_postage_stamp_requests
            set download_attempts = download_attempts+1,
            status = %s,
            updated = now()
            where name = %s
            """, (status, requestName))


   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))

   rowsUpdated = cursor.rowcount
   cursor.close ()

   return rowsUpdated


def addRequestToDatabase(conn, requestName, sqlCurrentDate, requestType = POSTAGE_STAMP_REQUEST):
   """addRequestToDatabase.

   Args:
       conn:
       requestName:
       sqlCurrentDate:
       requestType:
   """
   import MySQLdb
   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      cursor.execute ("""
          insert into tcs_postage_stamp_requests (name, pss_id, download_attempts, status, created, updated, request_type)
          values (%s, %s, %s, %s, %s, %s, %s)
          """, (requestName, None, 0, CREATED, sqlCurrentDate, None, requestType))

   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))

   cursor.close ()
   return conn.insert_id()



def updateRequestStatus(conn, requestName, status, pssId = None):
   """updateRequestStatus.

   Args:
       conn:
       requestName:
       status:
       pssId:
   """
   import MySQLdb

   rowsUpdated = 0
   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      # If the pssId is not supplied, check that it's not
      # already defined.
      if pssId is None:
         cursor.execute ("""
            select pss_id from tcs_postage_stamp_requests
            where name = %s
         """, (requestName,))

         if cursor.rowcount > 0:
            pssId = cursor.fetchone ()['pss_id']

      # Now do the update
      cursor.execute ("""
            update tcs_postage_stamp_requests
            set pss_id = %s,
            status = %s,
            updated = now()
            where name = %s
            """, (pssId, status, requestName))
      rowsUpdated = cursor.rowcount
      cursor.close ()


   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))

   return rowsUpdated


def addZooRequestToDatabase(conn, requestName, sqlCurrentDate):
   """addZooRequestToDatabase.

   Args:
       conn:
       requestName:
       sqlCurrentDate:
   """
   import MySQLdb
   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      cursor.execute ("""
          insert into tcs_zoo_requests (name, zoo_id, download_attempts, status, created, updated)
          values (%s, %s, %s, %s, %s, %s)
          """, (requestName, None, 0, CREATED, sqlCurrentDate, None))

   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))

   cursor.close ()
   return conn.insert_id()


def updateZooRequestStatus(conn, requestName, status, zooId = None):
   """updateZooRequestStatus.

   Args:
       conn:
       requestName:
       status:
       zooId:
   """
   import MySQLdb

   rowsUpdated = 0
   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      # If the zooId is not supplied, check that it's not
      # already defined. This bit of code allows this function
      # to be called in various circumstances.
      if zooId is None:
         cursor.execute ("""
            select zoo_id from tcs_zoo_requests
            where name = %s
         """, (requestName,))

         if cursor.rowcount > 0:
            zooId = cursor.fetchone ()['zoo_id']

      # Now do the update
      cursor.execute ("""
            update tcs_zoo_requests
            set zoo_id = %s,
            status = %s,
            updated = now()
            where name = %s
            """, (zooId, status, requestName))
      rowsUpdated = cursor.rowcount
      cursor.close ()


   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))

   return rowsUpdated


def addZooRequestIdToTransients(conn, zooReqeustId, transients):
   """addZooRequestIdToTransients.

   Args:
       conn:
       zooReqeustId:
       transients:
   """
   import MySQLdb

   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      # Zoo List ID = 6
      for transient in transients:
         cursor.execute ("""
            update tcs_transient_objects
            set zoo_request_id = %s, detection_list_id = 6
            where id = %s
            """, (zooReqeustId, transient))

      rowsUpdated = cursor.rowcount
      cursor.close ()

   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))

   return rowsUpdated


# Joel Welling code here - parse the index.txt file from the data store.
# Note that the Postage Stamp Server index file is different from the main
# data store index files.  Need to modify this to check for each comment line
# and create separate dictionaries for each one.

def parseIndexList(lines):
   """parseIndexList.

   Args:
       lines:
   """
   dictList = []
   result = []

   if len(lines) == 0:
      print("Empty file")
      return []

   if len(lines[0]) == 0:
      print("Can't process empty first line")
      return []

   if lines[0][0] != "#":
      print("Did not find at least one '#' in line 1 column 1 of downloaded file list")
      return []


   for line in lines:
      if len(line) == 0:
         continue

      if line[0] == '#':
         # This is the set of keys

         if lines.index(line) != 0:
            # Append the previous list of dicts
            dictList.append(result)
            result = []

         keys = line[1:].split('|')
         keys = ( k.strip() for k in keys )
         cleanKeys = []
         for k in keys:
            if len(k)>0:
               cleanKeys.append(k)
         # Reset lines to be the lines minus the current keys line
         lines = lines[lines.index(line)+1:]

      else:
         # This is a value
         line.strip()
         if len(line) > 0:
            words = line.strip().split('|')
            cleanWords = []
            for word in words:
               if len(word) > 0:
                  cleanWords.append(word)
            cleanWords= [word.strip() for word in cleanWords]
            if len(cleanWords) == 0:
               continue

            # In PSS results, if there is a failure in any of the images
            # we get a line with 5 keys, but only 4 words...  The "component"
            # keys is not populated.  We'll have to take account of this...
            if len(cleanWords) < len(cleanKeys) and cleanKeys[-1] == 'component':
               cleanKeys.remove(cleanKeys[-1])

            if len(cleanWords) < len(cleanKeys):
               print("Wrong length line %d in index list" % lines.index(line))
               return []

            lineDict = {}
            for i in range(len(cleanKeys)):
               lineDict[cleanKeys[i]] = cleanWords[i]
            result.append(lineDict)

   # Now append the last acquired list
   dictList.append(result)

   return dictList

#+----+--------------------------------+--------+-------------------+--------+---------------------+---------------------+
#| id | name                           | pss_id | download_attempts | status | created             | updated             |
#+----+--------------------------------+--------+-------------------+--------+---------------------+---------------------+
#| 22 | qub_ps_request_20091127_200720 |   NULL |                 0 |      0 | 2009-11-27 20:07:20 | NULL                | 
#| 23 | qub_ps_request_20091127_222535 |   NULL |                 0 |      0 | 2009-11-27 22:25:35 | NULL                | 
#| 24 | qub_ps_request_20091127_223413 |   NULL |                 0 |      0 | 2009-11-27 22:34:13 | NULL                | 
#| 25 | qub_ps_request_20091127_224050 |   1695 |                 0 |      1 | 2009-11-27 22:40:50 | 2009-11-27 22:40:55 | 
#+----+--------------------------------+--------+-------------------+--------+---------------------+---------------------+

# 2010-09-21 KWS Added requestType, so that we can re-use the function
# to download detectability requests independently of postage stamp requests
def getPSRequestList(conn, status, creationDate = '2009-11-01 00:00:00', requestType = POSTAGE_STAMP_REQUEST, limit = None):
   """getPSRequestList.

   Args:
       conn:
       status:
       creationDate:
       requestType:
       limit:
   """
   import MySQLdb
   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      if limit:
         cursor.execute ("""
            select * from tcs_postage_stamp_requests
            where status = %s
            and created > %s
            and request_type = %s
            limit %s
         """, (status, creationDate, requestType, limit))
      else:
         cursor.execute ("""
            select * from tcs_postage_stamp_requests
            where status = %s
            and created > %s
            and request_type = %s
         """, (status, creationDate, requestType))

      psRequestList = cursor.fetchall ()

      cursor.close ()

   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))
      sys.exit (1)

   return psRequestList


def getOrInsertImageGroup(conn, groupName, groupType=None):
   """getOrInsertImageGroup.

   Args:
       conn:
       groupName:
       groupType:
   """
   import MySQLdb

   rowsUpdated = 0

   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      if groupType is None:
         cursor.execute ("""
            select id from tcs_image_groups
            where name = %s
         """, (groupName,))
      else:
         cursor.execute ("""
            select id from tcs_image_groups
            where name = %s
            and group_type = %s
         """, (groupName, groupType))


      if cursor.rowcount > 0:
         groupId = cursor.fetchone ()['id']
      else:
         # We need to insert a new group
         cursor.execute ("""
            insert into tcs_image_groups (name, group_type)
            values (%s, %s)
            """, (groupName, groupType))
         groupId = conn.insert_id()

         (id, mjd, diffId, ippIdet) = groupName.split('_')

         # And we need to attach the newly created group to the associated object
         # NOTE: This strategem works fine for newly created images, but doesn't work
         #       for subsequent downloads of new image data for this observation.
         #       This may need to be re-visited.
         try:
            # Work out how many decimal places we need to truncate the
            # MJD to in our SQL query.
            (wholeMJD, fractionMJD) = mjd.split('.')
            numberOfDecimalPlaces = len(fractionMJD)
         except ValueError as e:
            numberOfDecimalPlaces = 0

         # Update tcs_transient_reobservations first (since we only
         # tend to download images of objects with multiple recurrences).
         # This should reduce the number of queries necessary.

         if groupType is None:  # Don't bother updating the transient recurrence if we're dealing with a finder

            cursor.execute ("""
               update tcs_transient_reobservations r, tcs_cmf_metadata m
               set r.image_group_id = %s
               where r.transient_object_id = %s
               and r.ipp_idet = %s
               and r.tcs_cmf_metadata_id = m.id
               and cast(truncate(m.mjd_obs, %s) as char) = %s
               and m.imageid = %s
               """, (groupId, id, ippIdet, numberOfDecimalPlaces, mjd, diffId))

            rowsUpdated = cursor.rowcount

            if rowsUpdated == 0:
               # Try updating the tcs_transient_objects table
               cursor.execute ("""
                  update tcs_transient_objects o, tcs_cmf_metadata m
                  set o.image_group_id = %s
                  where o.id = %s
                  and o.ipp_idet = %s
                  and o.tcs_cmf_metadata_id = m.id
                  and cast(truncate(m.mjd_obs, %s) as char) = %s
                  and m.imageid = %s
                  """, (groupId, id, ippIdet, numberOfDecimalPlaces, mjd, diffId))

               rowsUpdated = cursor.rowcount

               # Did we update any transient object rows? If not issue a warning.
               if rowsUpdated == 0:
                  print("WARNING: No transient object entries were updated.")
                  print("These images are not associated with an object in this database.")

      cursor.close ()

   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))
      sys.exit (1)

   # Did we update more than one row?  If so, the number of decimal
   # places we're using in the MJD is not fine-grained enough to
   # distinguish a unique observation.  Either that, or there is an
   # observation that has exactly the same timestamp and diff ID.
   if rowsUpdated > 1:
      print("WARNING: More than one object was updated.")
      print("The truncated MJD criteria are not fine enough.")

   return groupId


def insertPostageStampImageRecord(conn, imageName, pssName, imageMJD, pssErrorCode, filterId=None, maskedPixelRatio=None, maskedPixelRatioAtCore=None, groupType=None):
   """insertPostageStampImageRecord.

   Args:
       conn:
       imageName:
       pssName:
       imageMJD:
       pssErrorCode:
       filterId:
       maskedPixelRatio:
       maskedPixelRatioAtCore:
       groupType:
   """
   import MySQLdb
   # Find the image group
   imageId = 0
   imageGroupId = 0

   (id, mjd, diffid, ippIdet, imageType) = imageName.split('_')

   # 2014-03-10 KWS Need to append a suffix to finder groups.
   #                If this is a finder, we need to append something to the image name
   #                so that the database insert is not rejected (because of uniqueness)
   if 'finder' in imageType:
      ippIdet += 'f'

   imageGroupId = getOrInsertImageGroup(conn, '%s_%s_%s_%s' % (id, mjd, diffid, ippIdet), groupType=groupType)
   print("Image Group ID of image %s is: %s" % (imageName, imageGroupId))

   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      cursor.execute ("""
         select id from tcs_postage_stamp_images
         where image_filename = %s
      """, (imageName,))

      if cursor.rowcount > 0:
         # A record already exists.  Let's delete it and replace it with the new one.
         imageId = cursor.fetchone ()['id']
         cursor.execute("""
            delete from tcs_postage_stamp_images
            where id = %s
         """, (imageId,))

      cursor.execute ("""
         insert into tcs_postage_stamp_images (image_type, image_filename, pss_filename, mjd_obs, image_group_id, pss_error_code, filter, mask_ratio, mask_ratio_at_core)
         values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
         """, (imageType, imageName, pssName, imageMJD, imageGroupId, pssErrorCode, filterId, maskedPixelRatio, maskedPixelRatioAtCore))

      imageId = conn.insert_id()
      cursor.close ()

   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))
      sys.exit (1)

   return (imageId, imageGroupId)

# Add a function to extract image trios for the Supernova Zoo

def extractImageTrioForZoo(conn, candidate):
   """extractImageTrioForZoo.

   Args:
       conn:
       candidate:
   """
   import MySQLdb
   imageList = []
   if candidate != '':
      candidate += '%'

   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      # Pick out the images where there are no errors and order by
      # ascending core and whole number of NaNs.  Pick out the
      # first image (i.e. one with the least number of NaNs).
      cursor.execute ("""
         select image_filename from tcs_postage_stamp_images
         where image_group_id in (
            select image_group_id from (
               select * from (
                  select g.name, 
                         image_group_id,
                         sum(mask_ratio_at_core) core,
                         sum(mask_ratio) whole,
                         sum(pss_error_code) errors
                  from tcs_postage_stamp_images i, tcs_image_groups g
                 where image_filename like %s and i.image_group_id = g.id
              group by image_group_id
                having errors = 0 and core < 0.9
              order by core, whole
                  ) temp limit 1
               ) temp2
            )
         order by image_filename desc
      """, (candidate,))
      images = cursor.fetchall ()

      for row in images:
         imageList.append(row["image_filename"])

      cursor.close ()

   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))
      sys.exit (1)

   return imageList


def getObjectRecurrences(conn, objectId):
   """
   Get all object occurrences.
   """
   import MySQLdb

   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      # Note that DEC is a MySQL reserved word, so need quotes around it

      # 2011-10-12 KWS Addded imageid to order by clause to make sure that the results
      #                are ordered consistently
      cursor.execute ("""
            SELECT d.ra_psf RA,d.dec_psf 'DEC', m.imageid,
            substr(m.fpa_filter,1,1) Filter ,m.mjd_obs MJD,m.filename Filename,d.quality_threshold_pass QTP
            FROM tcs_transient_objects d, tcs_cmf_metadata m
            where d.id=%s
            and d.tcs_cmf_metadata_id = m.id
            UNION ALL
            SELECT d.ra_psf RA,d.dec_psf 'DEC', m.imageid,
            substr(m.fpa_filter,1,1) Filter ,m.mjd_obs MJD,m.filename Filename,d.quality_threshold_pass QTP
            FROM tcs_transient_reobservations d, tcs_cmf_metadata m
            where d.transient_object_id=%s
            and d.tcs_cmf_metadata_id = m.id
            ORDER by MJD, imageid
      """, (objectId, objectId))
      result_set = cursor.fetchall ()

      cursor.close ()

   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))

   return result_set


def getObjectNames(conn, objectId):
   """
   Get the object names.
   """
   import MySQLdb

   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      # Note that DEC is a MySQL reserved word, so need quotes around it

      # 2011-10-12 KWS Addded imageid to order by clause to make sure that the results
      #                are ordered consistently
      cursor.execute ("""
            SELECT local_designation, ps1_designation
              FROM tcs_transient_objects
             WHERE id = %s
      """, (objectId,))
      result_set = cursor.fetchone ()

      cursor.close ()

   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))

   return result_set



def downloadPostageStampImage(conn, requestName, fitsRow, dataStoreFileInfo, PSSImageRootLocation, dataStoreURL = None):
   """downloadPostageStampImage.

   Args:
       conn:
       requestName:
       fitsRow:
       dataStoreFileInfo:
       PSSImageRootLocation:
       dataStoreURL: The location of the data store.
   """
   downloadStatus = False
   errorCode = fitsRow.field('ERROR_CODE')

   # ImageMJD must be extracted from the image data.

   imageMJD = None
   filterId = None
   maskedPixelRatio = None
   maskedPixelRatioAtCore = None

   localImageName = fitsRow.field('COMMENT')
   pssImageName = fitsRow.field('IMG_NAME')

   # 2011-07-23 KWS Need to disable flipping for V3 images.
   flip = False
   if ".V3." in pssImageName:
      flip = False
   elif ".V2." in pssImageName:
      flip = True
   elif "MD" in pssImageName and ".V" not in pssImageName:
      flip = True

   # We need the MJD to create the directory
   (id, mjd, diffid, ippIdet, imageType) = localImageName.split('_')

   # 2013-09-23 KWS Need to propagate crosshair colours: green1 = detection, brown1 = non-detection
   if ippIdet == str(IPP_IDET_NON_DETECTION_VALUE):
      xhColor = 'brown1'
   else:
      xhColor = 'green1'

   imageDownloadLocation = PSSImageRootLocation + '/' + "%d" % int(eval(mjd))

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

   # 2010-11-25 KWS Changed the code to comment out the check for image existence.  It's nice
   # to be able to skip downloading the file if it aready exists, but this prevents postage
   # stamps from being re-generated.  It also screws up the following MD5 sum and removes
   # the FITS file (whilst preserving the previously generated JPEG).  So next line now commented out.
   #if not os.path.exists(absoluteLocalImageName):

   urllibFilename = None
   urllibHeaders = None

   try:
      (urllibFilename, urllibHeaders) = urllib.request.urlretrieve(dataStoreURL +requestName +'/' + dataStoreFileInfo['fileID'], absoluteLocalImageName)

   except IOError as e:
      print("ERROR: Image failed to download. Error is: %s. Having another go..." % e.errno)
      try:
         (urllibFilename, urllibHeaders) = urllib.request.urlretrieve(dataStoreURL +requestName +'/' + dataStoreFileInfo['fileID'], absoluteLocalImageName)

      except IOError as e:
         print("ERROR: Image failed to download. Error is: %s. Recording error." % e.errno)
         (imageId, imageGroupId) = insertPostageStampImageRecord(conn, localImageName, None, imageMJD, ERROR_COULD_NOT_DOWNLOAD, filterId, maskedPixelRatio, maskedPixelRatioAtCore)
         return downloadStatus


   # Check the MD5 sum.  If it fails, don't bother continuing.
   # Comment out the following block of code to skip MD5 check.

#   if md5(absoluteLocalImageName) != dataStoreFileInfo['md5sum']:
#      print "The MD5 check has failed.  This file did not download correctly."
#      # 2011-04-26 KWS Very occasionally urllib does NOT retrieve the file.  I think
#      #                the problem may be a redirection issue.  When the file fails
#      #                to download, os.remove throws an exception.  I still don't
#      #                understand what's causing the problem.  Will have to monitor
#      #                this one for a while before coming up with a permanent solution.
#      #                Some testing doesn't seem to reveal anything significantly
#      #                problemmatic.
#      if os.path.exists(absoluteLocalImageName):
#         os.remove(absoluteLocalImageName)
#      else:
#         print "File %s did not download!" % absoluteLocalImageName
#         print "Header Info from Urllib:"
#         print urllibHeaders
#
#      downloadStatus = False
#      (imageId, imageGroupId) = insertPostageStampImageRecord(conn, localImageName, None, imageMJD, ERROR_BAD_FILE_CHECKSUM, filterId, maskedPixelRatio, maskedPixelRatioAtCore)
#      return downloadStatus


   # Open image & extract image MJD. Note that the images are NORMALLY compressed,
   # though occasionally they come through uncompressed.  Need to take account of
   # this.

   try:
      hdus = pf.open(absoluteLocalImageName)
   except IOError as e:
      print("Problem with opening image %s" % absoluteLocalImageName)
      print(e)
      return downloadStatus
   except OSError as e:
      print("Problem with opening image %s" % absoluteLocalImageName)
      print(e)
      return downloadStatus

   header = []
   try:
      header = hdus[1].header
   
   except IndexError as e:
      print("This looks unpacked.  Try opening it as unpacked...")
      header = hdus[0].header

   imageMJD = header['MJD-OBS']
   filterId = header['FPA.FILTERID']

   # Convert to JPEG & add frame & crosshairs - write image
   # 2011-07-23 KWS Need to disable flipping for V3 images.
   (maskedPixelRatio, maskedPixelRatioAtCore) = imu.convertFitsToJpegWithCrosshairs(absoluteLocalImageName, imu.fitsToJpegExtension(absoluteLocalImageName), flip = flip, xhColor = xhColor)

   # Create an image record using IMG_NAME and COMMENT
   # This call also makes the association of the the image group with the
   # object in tcs_transient_objects and tcs_transient_reobservations.
   (imageId, imageGroupId) = insertPostageStampImageRecord(conn, localImageName, pssImageName, imageMJD, errorCode, filterId, maskedPixelRatio, maskedPixelRatioAtCore)

   downloadStatus = True

   return downloadStatus

# 2014-03-08 KWS Added a new method to download Finder images
def downloadFinderCMF(conn, requestName, fitsRow, dataStoreFileInfo, PSSImageRootLocation, dataStoreURL = None):
   """Download the CMF file associated with a Finder"""

   downloadStatus = False

   # ImageMJD must be extracted from the image data.

   localImageName = fitsRow.field('COMMENT') + 'finder'
   pssImageName = fitsRow.field('IMG_NAME')

   # We need the MJD to create the directory
   (id, mjd, diffid, ippIdet, imageType) = localImageName.split('_')

   imageDownloadLocation = PSSImageRootLocation + '/' + "%d" % int(eval(mjd))

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

   absoluteLocalCMFName = imageDownloadLocation + '/' + localImageName + '.cmf'

   urllibFilename = None
   urllibHeaders = None

   try:
      (urllibFilename, urllibHeaders) = urllib.request.urlretrieve(dataStoreURL +requestName +'/' + dataStoreFileInfo['fileID'], absoluteLocalCMFName)

   except IOError as e:
      print("ERROR: Image failed to download. Error is: %s. Having another go..." % e.errno)
      try:
         (urllibFilename, urllibHeaders) = urllib.request.urlretrieve(dataStoreURL +requestName +'/' + dataStoreFileInfo['fileID'], absoluteLocalCMFName)
      except IOError as e:
         print("ERROR: Image failed to download. Error is: %s. Recording error." % e.errno)
         return downloadStatus

   # Check the MD5 sum.  If it fails, don't bother continuing.
   if md5(absoluteLocalCMFName) != dataStoreFileInfo['md5sum']:
      print("The MD5 check has failed.  This file did not download correctly.")
      # 2011-04-26 KWS Very occasionally urllib does NOT retrieve the file.  I think
      #                the problem may be a redirection issue.  When the file fails
      #                to download, os.remove throws an exception.  I still don't
      #                understand what's causing the problem.  Will have to monitor
      #                this one for a while before coming up with a permanent solution.
      #                Some testing doesn't seem to reveal anything significantly
      #                problemmatic.
      if os.path.exists(absoluteLocalCMFName):
         os.remove(absoluteLocalCMFName)
      else:
         print("File %s did not download!" % absoluteLocalCMFName)
         print("Header Info from Urllib:")
         print(urllibHeaders)

      downloadStatus = False
   else:
      downloadStatus = True

   return downloadStatus


# 2014-03-08 KWS Added a new method to download Finder images
def downloadFinderImage(conn, requestName, fitsRow, dataStoreFileInfo, PSSImageRootLocation, offsetStarSearchRadius = 120.0, offsetStarMagThreshold = 15.0, dataStoreURL = None):
   """downloadFinderImage.

   Args:
       conn:
       requestName:
       fitsRow:
       dataStoreFileInfo:
       PSSImageRootLocation:
       offsetStarSearchRadius:
       offsetStarMagThreshold:
       dataStoreURL: The location of the data store.
   """
   from gkutils.commonutils import bruteForceCMFConeSearch, calculateRMSScatter

   downloadStatus = False
   errorCode = fitsRow.field('ERROR_CODE')

   # ImageMJD must be extracted from the image data.

   imageMJD = None
   filterId = None
   maskedPixelRatio = None
   maskedPixelRatioAtCore = None

   localImageName = fitsRow.field('COMMENT') + 'finder'
   pssImageName = fitsRow.field('IMG_NAME')

   pixelScale = 0.25 #arcsec per pixel

   # 2011-07-23 KWS Need to disable flipping for V3 images.
   flip = False
   if ".V3." in pssImageName:
      flip = False
   elif ".V2." in pssImageName:
      flip = True
      pixelScale = 0.2
   elif "MD" in pssImageName and ".V" not in pssImageName:
      flip = True
      pixelScale = 0.2

   # We need the MJD to create the directory
   (id, mjd, diffid, ippIdet, imageType) = localImageName.split('_')

   # 2013-09-23 KWS Need to propagate crosshair colours: green1 = detection, brown1 = non-detection
   if ippIdet == str(IPP_IDET_NON_DETECTION_VALUE):
      xhColor = 'brown1'
   else:
      xhColor = 'green1'

   imageDownloadLocation = PSSImageRootLocation + '/' + "%d" % int(eval(mjd))

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

   # We need to look into the CMF file for nearby stars. This will have been saved previously.
   absoluteLocalCMFName = imageDownloadLocation + '/' + localImageName + '.cmf'

   urllibFilename = None
   urllibHeaders = None

   try:
      (urllibFilename, urllibHeaders) = urllib.request.urlretrieve(dataStoreURL +requestName +'/' + dataStoreFileInfo['fileID'], absoluteLocalImageName)

   except IOError as e:
      print("ERROR: Image failed to download. Error is: %s. Having another go..." % e.errno)
      try:
         (urllibFilename, urllibHeaders) = urllib.request.urlretrieve(dataStoreURL +requestName +'/' + dataStoreFileInfo['fileID'], absoluteLocalImageName)

      except IOError as e:
         print("ERROR: Image failed to download. Error is: %s. Recording error." % e.errno)
         (imageId, imageGroupId) = insertPostageStampImageRecord(conn, localImageName, None, imageMJD, ERROR_COULD_NOT_DOWNLOAD, filterId, maskedPixelRatio, maskedPixelRatioAtCore, groupType=GROUP_TYPE_FINDER)
         return downloadStatus


   # Check the MD5 sum.  If it fails, don't bother continuing.
   if md5(absoluteLocalImageName) != dataStoreFileInfo['md5sum']:
      print("The MD5 check has failed.  This file did not download correctly.")
      # 2011-04-26 KWS Very occasionally urllib does NOT retrieve the file.  I think
      #                the problem may be a redirection issue.  When the file fails
      #                to download, os.remove throws an exception.  I still don't
      #                understand what's causing the problem.  Will have to monitor
      #                this one for a while before coming up with a permanent solution.
      #                Some testing doesn't seem to reveal anything significantly
      #                problemmatic.
      if os.path.exists(absoluteLocalImageName):
         os.remove(absoluteLocalImageName)
      else:
         print("File %s did not download!" % absoluteLocalImageName)
         print("Header Info from Urllib:")
         print(urllibHeaders)

      downloadStatus = False
      (imageId, imageGroupId) = insertPostageStampImageRecord(conn, localImageName, None, imageMJD, ERROR_BAD_FILE_CHECKSUM, filterId, maskedPixelRatio, maskedPixelRatioAtCore, groupType=GROUP_TYPE_FINDER)
      return downloadStatus


   # Open image & extract image MJD. Note that the images are NORMALLY compressed,
   # though occasionally they come through uncompressed.  Need to take account of
   # this.

   hdus = pf.open(absoluteLocalImageName)
   header = []
   try:
      header = hdus[1].header
   
   except IndexError as e:
      print("This looks unpacked.  Try opening it as unpacked...")
      header = hdus[0].header

   imageMJD = header['MJD-OBS']
   filterId = header['FPA.FILTERID']

   # Search for nearest objects in associated CMF file. Note that for this to work, the CMF
   # must have been downloaded already. Note that a cursory examination of the results file
   # appears to show that CMF files apppear first, but it might be prudent to do two sweeps.
   # First sweep downloads the CMF files.  Second sweep downloads the finder images.
   # We'll try getting the CMF stars twice.  We generally only want stars brighter than 15.0
   # but if necessary we'll revise this down to 16.0.  If we can't find any stars, we'll not
   # bother reporting any.

   # Get the object info and average coords.
   objectRecurrences = getObjectRecurrences(conn, int(id))
   objectNames = getObjectNames(conn, int(id))
   avgRa, avgDec, rms = calculateRMSScatter(objectRecurrences)

   objectInfo = {}
   objectName = None
   if objectNames['ps1_designation']:
      objectName = objectNames['ps1_designation']
   elif objectNames['local_designation']:
      objectName = objectNames['local_designation']
   else:
      objectName = id

   objectInfo['name'] = objectName
   objectInfo['ra'] = avgRa
   objectInfo['dec'] = avgDec
   objectInfo['filter'] = filterId

   nearbyObjects = []

   # Does the CMF file exist?

   if os.path.exists(absoluteLocalCMFName):

      header, results = bruteForceCMFConeSearch(absoluteLocalCMFName, [[objectInfo['ra'], objectInfo['dec']]], offsetStarSearchRadius)
      headerCols = header.split()
      for tableRow in results:
         rowDict = {}
         row = tableRow.split()
         for i, column in enumerate(headerCols):
             rowDict[column] = row[i]
         nearbyObjects.append(rowDict)

      refStars = []

      # Try THREE times to find ref stars. Exclude objects too close to the transient.
      minDistance = 5.0 # arcsec
      maxNumberOfStars = 5

      for row in nearbyObjects:
         if float(row['CAL_PSF_MAG']) < offsetStarMagThreshold and float(row['separation']) > minDistance:
            refStars.append(row)
            xmObjects.remove(row)
            if len(refStars) >= 5:
               break

      if len(refStars) < 5:
         print("No ref stars of mag %s within specified radius %s arcsec. Attempting another search with fainter mag threshold of %s." % (offsetStarMagThreshold, offsetStarSearchRadius, offsetStarMagThreshold + 1.0))
         for row in nearbyObjects:
            if float(row['CAL_PSF_MAG']) < offsetStarMagThreshold + 1.0 and float(row['separation']) > minDistance:
               refStars.append(row)
               xmObjects.remove(row)
            if len(refStars) >= 5:
               break

      if len(refStars) < 5:
         print("No ref stars of mag %s within specified radius %s arcsec. Attempting another search with fainter mag threshold of %s." % (offsetStarMagThreshold + 1.0, offsetStarSearchRadius, offsetStarMagThreshold + 2.0))
         for row in nearbyObjects:
            if float(row['CAL_PSF_MAG']) < offsetStarMagThreshold + 2.0 and float(row['separation']) > minDistance:
               refStars.append(row)
               xmObjects.remove(row)
            if len(refStars) >= 5:
               break

      if len(refStars) < 5:
         print("No ref stars of mag %s within specified radius %s arcsec. Attempting another search with fainter mag threshold of %s." % (offsetStarMagThreshold + 2.0, offsetStarSearchRadius, offsetStarMagThreshold + 3.0))
         for row in nearbyObjects:
            if float(row['CAL_PSF_MAG']) < offsetStarMagThreshold + 3.0 and float(row['separation']) > minDistance:
               refStars.append(row)
               xmObjects.remove(row)
            if len(refStars) >= 5:
               break

      if not refStars:
         print("No ref stars of mag %s within specified radius %s arcsec." % (offsetStarMagThreshold + 3.0, offsetStarSearchRadius))
   else:
      print("No CMF (catalogue) file for this finder.")


   (maskedPixelRatio, maskedPixelRatioAtCore) = imu.convertFitsToJpegWithCrosshairs(absoluteLocalImageName, imu.fitsToJpegExtension(absoluteLocalImageName), flip = flip, xhColor = xhColor, negate = True, nsigma = 3, objectInfo = objectInfo, standardStars = refStars, pixelScale = pixelScale)

   # Create an image record using IMG_NAME and COMMENT
   # This call also makes the association of the the image group with the
   # object in tcs_transient_objects and tcs_transient_reobservations.
   (imageId, imageGroupId) = insertPostageStampImageRecord(conn, localImageName, pssImageName, imageMJD, errorCode, filterId, maskedPixelRatio, maskedPixelRatioAtCore, groupType = GROUP_TYPE_FINDER)

   downloadStatus = True

   return downloadStatus

# 2015-03-16 KWS Can't test the image conversion process if the stamp server is down!
#                So - I've abstracted the image conversion code into a separate method.
# 2015-03-18 KWS Override finder filter (e.g. with r-band).

def createFinderImage(conn, absoluteLocalImageName, objectInfo = {}, xhColor = 'green1', flip = False, offsetStarSearchRadius = 120.0, offsetStarMagThreshold = 15.0, pixelScale = 0.25, offsetStarFilter = None, nsigma = 2):
   """createFinderImage.

   Args:
       conn:
       absoluteLocalImageName:
       objectInfo:
       xhColor:
       flip:
       offsetStarSearchRadius:
       offsetStarMagThreshold:
       pixelScale:
       offsetStarFilter:
   """
   from gkutils.commonutils import coneSearchHTM, FULL, sky2xy, getOffset

   # Open image & extract image MJD. Note that the images are NORMALLY compressed,
   # though occasionally they come through uncompressed.  Need to take account of
   # this.

   hdus = pf.open(absoluteLocalImageName)
   header = []
   try:
      header = hdus[1].header
   
   except IndexError as e:
      print("This looks unpacked.  Try opening it as unpacked...")
      header = hdus[0].header

   imageMJD = header['MJD-OBS']
   filterId = header['FPA.FILTERID']

   if offsetStarFilter is None:
      offsetStarFilter = filterId[0]

   if offsetStarFilter not in "grizy":
      offsetStarFilter = 'r'


   # Search for nearest objects in associated CMF file. Note that for this to work, the CMF
   # must have been downloaded already. Note that a cursory examination of the results file
   # appears to show that CMF files apppear first, but it might be prudent to do two sweeps.
   # First sweep downloads the CMF files.  Second sweep downloads the finder images.
   # We'll try getting the CMF stars twice.  We generally only want stars brighter than 15.0
   # but if necessary we'll revise this down to 16.0.  If we can't find any stars, we'll not
   # bother reporting any.


   catalogueName = 'tcs_cat_ps1_dr1'

   message = None
   xmObjects = []

   if objectInfo:
      objectInfo['filter'] = filterId[0]
      message, xmObjects = coneSearchHTM(objectInfo['ra'], objectInfo['dec'], offsetStarSearchRadius, catalogueName, queryType = FULL, conn = conn)

   searchDone = False
   # Did we search the catalogues correctly?
   if message and (message.startswith('Error') or 'not recognised' in message):
      # Successful cone searches should not return an error message, otherwise something went wrong.
      print("Database error - cone search unsuccessful.  Message was:")
      print("\t%s" % message)
      searchDone = False
   else:
      searchDone = True

   refStars = []
   if searchDone and xmObjects:

      numberOfMatches = len(xmObjects)
      print("Number of stars = %d" % numberOfMatches)

      #for row in xmObjects:
      #   print(row[0], row[1][offsetStarFilter + 'PSFMag'])

      # In the first draft of the code, just make the results look
      # like the results from the brute force CMF search.  We can
      # do further fixes later.  We'll also arbitrarily choose the
      # r-band magnitudes initially.  Eventually we'll choose
      # whatever filter was in the image we requested.

      # Try THREE times to find ref stars. Exclude objects too close to the transient.
      minDistance = 5.0 # arcsec
      maxNumberOfStars = 5

      for row in xmObjects:
         if row[1][offsetStarFilter + 'PSFMag'] and row[1]['ps_score'] and float(row[1][offsetStarFilter + 'PSFMag']) < offsetStarMagThreshold and float(row[1]['ps_score']) > 0.9 and float(row[0]) > minDistance:
            refStars.append(row)
            xmObjects.remove(row)
            if len(refStars) >= 5:
               break

      if len(refStars) < 5:
         print("No ref stars of mag %s within specified radius %s arcsec. Attempting another search with fainter mag threshold of %s." % (offsetStarMagThreshold, offsetStarSearchRadius, offsetStarMagThreshold + 1.0))
         for row in xmObjects:
            if row[1][offsetStarFilter + 'PSFMag'] and row[1]['ps_score'] and float(row[1][offsetStarFilter + 'PSFMag']) < offsetStarMagThreshold + 1.0 and float(row[1]['ps_score']) > 0.9 and float(row[0]) > minDistance:
               refStars.append(row)
               xmObjects.remove(row)
            if len(refStars) >= 5:
               break

      if len(refStars) < 5:
         print("No ref stars of mag %s within specified radius %s arcsec. Attempting another search with fainter mag threshold of %s." % (offsetStarMagThreshold + 1.0, offsetStarSearchRadius, offsetStarMagThreshold + 2.0))
         for row in xmObjects:
            if row[1][offsetStarFilter + 'PSFMag'] and row[1]['ps_score'] and float(row[1][offsetStarFilter + 'PSFMag']) < offsetStarMagThreshold + 2.0 and float(row[1]['ps_score']) > 0.9 and float(row[0]) > minDistance:
               refStars.append(row)
               xmObjects.remove(row)
            if len(refStars) >= 5:
               break

      if len(refStars) < 5:
         print("No ref stars of mag %s within specified radius %s arcsec. Attempting another search with fainter mag threshold of %s." % (offsetStarMagThreshold + 2.0, offsetStarSearchRadius, offsetStarMagThreshold + 3.0))
         for row in xmObjects:
            if row[1][offsetStarFilter + 'PSFMag'] and row[1]['ps_score'] and float(row[1][offsetStarFilter + 'PSFMag']) < offsetStarMagThreshold + 3.0 and float(row[1]['ps_score']) > 0.9 and float(row[0]) > minDistance:
               refStars.append(row)
               xmObjects.remove(row)
            if len(refStars) >= 5:
               break

      if not refStars:
         print("No ref stars of mag %s within specified radius %s arcsec." % (offsetStarMagThreshold + 3.0, offsetStarSearchRadius))
   else:
      print("No Ubercal Stars for this finder!")

   convertedRefStars = []
   if refStars:
      # To avoid major revisions to image_utils, convert refStars so that it
      # has the same key value pairs as before
      # 2015-03-18 KWS Add the NE location of the offset stars
      for row in refStars:
         x, y = sky2xy(absoluteLocalImageName, row[1]['raAve'], row[1]['decAve'])
         star = {}
         star['RA_J2000'] = row[1]['raAve']
         star['DEC_J2000'] = row[1]['decAve']
         star['X_PSF'] = float(x)
         star['Y_PSF'] = float(y)
         star['CAL_PSF_MAG'] = row[1][offsetStarFilter + 'PSFMag']
         star['filter'] = offsetStarFilter
         star['offset'] = getOffset(row[1]['raAve'], row[1]['decAve'], objectInfo['ra'], objectInfo['dec'])

         convertedRefStars.append(star)

   (maskedPixelRatio, maskedPixelRatioAtCore) = imu.convertFitsToJpegWithCrosshairs2(absoluteLocalImageName, imu.fitsToJpegExtension(absoluteLocalImageName), flip = flip, xhColor = xhColor, negate = True, nsigma = nsigma, objectInfo = objectInfo, standardStars = convertedRefStars, pixelScale = pixelScale)

   imageDetails = {}
   imageDetails['maskedPixelRatio'] = maskedPixelRatio
   imageDetails['maskedPixelRatioAtCore'] = maskedPixelRatioAtCore
   imageDetails['filterId'] = filterId
   imageDetails['imageMJD'] = imageMJD

   return imageDetails

# 2015-02-24 KWS Download finder images, but use PS1 Ubercal Star catalog.
# 2015-03-18 KWS Override finder filter (e.g. with r-band).
def downloadFinderImage2(conn, requestName, fitsRow, dataStoreFileInfo, PSSImageRootLocation, offsetStarSearchRadius = 120.0, offsetStarMagThreshold = 15.0, offsetStarFilter = None, dataStoreURL = None, nsigma = 2.0, connSherlock = None):
   """downloadFinderImage2.

   Args:
       conn:
       requestName:
       fitsRow:
       dataStoreFileInfo:
       PSSImageRootLocation:
       offsetStarSearchRadius:
       offsetStarMagThreshold:
       offsetStarFilter:
       dataStoreURL: The location of the data store.
       connSherlock:
   """
   from gkutils.commonutils import calculateRMSScatter

   if connSherlock is None:
      connSherlock = conn

   downloadStatus = False
   errorCode = fitsRow.field('ERROR_CODE')

   # ImageMJD must be extracted from the image data.

   imageMJD = None
   filterId = None
   maskedPixelRatio = None
   maskedPixelRatioAtCore = None

   localImageName = fitsRow.field('COMMENT') + 'finder'
   pssImageName = fitsRow.field('IMG_NAME')

   pixelScale = 0.25 #arcsec per pixel

   # 2011-07-23 KWS Need to disable flipping for V3 images.
   flip = False
   if ".V3." in pssImageName:
      flip = False
   elif ".V2." in pssImageName:
      flip = True
      pixelScale = 0.2
   elif "MD" in pssImageName and ".V" not in pssImageName:
      flip = True
      pixelScale = 0.2

   # We need the MJD to create the directory
   (id, mjd, diffid, ippIdet, imageType) = localImageName.split('_')

   # 2013-09-23 KWS Need to propagate crosshair colours: green1 = detection, brown1 = non-detection
   if ippIdet == str(IPP_IDET_NON_DETECTION_VALUE):
      xhColor = 'brown1'
   else:
      xhColor = 'green1'

   imageDownloadLocation = PSSImageRootLocation + '/' + "%d" % int(eval(mjd))

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

   # We need to look into the CMF file for nearby stars. This will have been saved previously.
   absoluteLocalCMFName = imageDownloadLocation + '/' + localImageName + '.cmf'

   urllibFilename = None
   urllibHeaders = None

   try:
      (urllibFilename, urllibHeaders) = urllib.request.urlretrieve(dataStoreURL +requestName +'/' + dataStoreFileInfo['fileID'], absoluteLocalImageName)

   except IOError as e:
      print("ERROR: Image failed to download. Error is: %s. Having another go..." % e.errno)
      try:
         (urllibFilename, urllibHeaders) = urllib.request.urlretrieve(dataStoreURL +requestName +'/' + dataStoreFileInfo['fileID'], absoluteLocalImageName)

      except IOError as e:
         print("ERROR: Image failed to download. Error is: %s. Recording error." % e.errno)
         (imageId, imageGroupId) = insertPostageStampImageRecord(conn, localImageName, None, imageMJD, ERROR_COULD_NOT_DOWNLOAD, filterId, maskedPixelRatio, maskedPixelRatioAtCore, groupType=GROUP_TYPE_FINDER)
         return downloadStatus


   # Check the MD5 sum.  If it fails, don't bother continuing.
   if md5(absoluteLocalImageName) != dataStoreFileInfo['md5sum']:
      print("The MD5 check has failed.  This file did not download correctly.")
      # 2011-04-26 KWS Very occasionally urllib does NOT retrieve the file.  I think
      #                the problem may be a redirection issue.  When the file fails
      #                to download, os.remove throws an exception.  I still don't
      #                understand what's causing the problem.  Will have to monitor
      #                this one for a while before coming up with a permanent solution.
      #                Some testing doesn't seem to reveal anything significantly
      #                problemmatic.
      if os.path.exists(absoluteLocalImageName):
         os.remove(absoluteLocalImageName)
      else:
         print("File %s did not download!" % absoluteLocalImageName)
         print("Header Info from Urllib:")
         print(urllibHeaders)

      downloadStatus = False
      (imageId, imageGroupId) = insertPostageStampImageRecord(conn, localImageName, None, imageMJD, ERROR_BAD_FILE_CHECKSUM, filterId, maskedPixelRatio, maskedPixelRatioAtCore, groupType=GROUP_TYPE_FINDER)
      return downloadStatus

   # 2015-03-24 KWS Get the image information BEFORE passing to createFinderImage.
   #                This allows createFinderImage to be called by non-PS1 related
   #                code (e.g. PESSTO). 
   # Get the object info and average coords.
   objectRecurrences = getObjectRecurrences(conn, int(id))
   objectNames = getObjectNames(conn, int(id))
   avgRa, avgDec, rms = calculateRMSScatter(objectRecurrences)

   objectInfo = {}
   objectName = None
   if objectNames['ps1_designation']:
      objectName = objectNames['ps1_designation']
   elif objectNames['local_designation']:
      objectName = objectNames['local_designation']
   else:
      objectName = id

   objectInfo['name'] = objectName
   objectInfo['ra'] = avgRa
   objectInfo['dec'] = avgDec

   imageDetails = createFinderImage(connSherlock, absoluteLocalImageName, objectInfo, xhColor = xhColor, flip = flip, offsetStarSearchRadius = offsetStarSearchRadius, offsetStarMagThreshold = offsetStarMagThreshold, pixelScale = pixelScale, offsetStarFilter = offsetStarFilter, nsigma = nsigma)

   # Create an image record using IMG_NAME and COMMENT
   # This call also makes the association of the the image group with the
   # object in tcs_transient_objects and tcs_transient_reobservations.
   (imageId, imageGroupId) = insertPostageStampImageRecord(conn, localImageName, pssImageName, imageDetails['imageMJD'], errorCode, imageDetails['filterId'], imageDetails['maskedPixelRatio'], imageDetails['maskedPixelRatioAtCore'], groupType = GROUP_TYPE_FINDER)

   downloadStatus = True

   return downloadStatus


# 2023-10-03 KWS Will move this eventually to requests. In the meantime for python 3 need
#                to add .decode('utf-8').

def getDataStoreIndex(requestName, dataStoreURL = None):
   """getDataStoreIndex.

   Args:
       requestName:
       dataStoreURL:
   """
   url = dataStoreURL + requestName + '/index.txt'
   responseErrorCode = OK
   dsResponsePage = ''

   try:
      req = urllib.request.Request(url)
      dsResponsePage = urllib.request.urlopen(req).read().decode('utf-8')

   except urllib.error.HTTPError as e:
      if e.code == 404:
         print("Page not found. Perhaps the server has not processed the request yet")
         responseErrorCode = PAGE_NOT_FOUND
      else:      
         print(e)
         responseErrorCode = HTTP_ERROR

   except urllib.error.URLError as e:
      print("Bad URL")
      responseErrorCode = BAD_SERVER_ADDRESS

   return (dsResponsePage, responseErrorCode)

def findDictInDictList(dictList, value):
   """findDictInDictList.

   Args:
       dictList:
       value:
   """
   # This is extremely inefficient code...  We need to get the info pertaining to a specific filename
   dictVal = {}

   for row in dictList:
      if row['fileID'] == value:
         dictVal = row

   return dictVal

# ###########################################################################################
# ************************* Postage Stamp Request Code added here ***************************
# ###########################################################################################
# 2010-02-16 KWS Altered query to allow requests to be made for images
#                for previously requested objects that have ongoing
#                recurrent observations.
# 2010-02-25 KWS Altered query to not pick up candidates marked as garbage.
# 2010-05-21 KWS Undone the changes of 2010-02-16 because this causes
#                repeated requests of the same image data if the response
#                images have not yet been downloaded.  We need to come back
#                to this... Note the SQL comment lines --
#                This is vital for automation to stop images from being
#                requested multiple times.

def candidateList(conn, candidateFlags = -1, detectionList = 4):
   """candidateList.

   Args:
       conn:
       candidateFlags:
       detectionList:
   """
   import MySQLdb
   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      # NOTE: To speed up requests, we've been asked to order them by
      #       skycell.  Ordering by filename also equals ordering by
      #       skycell.
      cursor.execute ("""
         select o.id from tcs_transient_objects o
         join tcs_cmf_metadata m
         on o.tcs_cmf_metadata_id = m.id
         left join tcs_postage_stamp_requests p
         on (p.id = o.postage_stamp_request_id)
         where o.followup_id is not null
         and o.detection_list_id = %s
         and o.detection_list_id != 0
         and (o.postage_stamp_request_id is null
--             or (o.postage_stamp_request_id is not null
--            and o.id in (select distinct transient_object_id
--                           from tcs_transient_reobservations
--                          where image_group_id is null))
         or (o.postage_stamp_request_id is not null
             and p.pss_id is null)
--             and p.pss_id > 0)
             )
         and o.object_classification & %s > 0
         order by m.filename, o.followup_id
      """, (detectionList, candidateFlags))
      candidateArray = []
      candidates = cursor.fetchall ()

      for row in candidates:
         candidateArray.append(row["id"])

      cursor.close ()

   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))
      sys.exit (1)

   return candidateArray

# Get all the candidates that have SOME images, but new detections that need an image
def candidateListForUpdate(conn, candidateFlags = -1, detectionList = 4):
   """candidateListForUpdate.

   Args:
       conn:
       candidateFlags:
       detectionList:
   """
   import MySQLdb
   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      # NOTE: To speed up requests, we've been asked to order them by
      #       skycell.  Ordering by filename also equals ordering by
      #       skycell.
      cursor.execute ("""
         select o.id from tcs_transient_objects o, tcs_cmf_metadata m
         where o.tcs_cmf_metadata_id = m.id
         and o.followup_id is not null
         and o.detection_list_id = %s
         and o.detection_list_id != 0
         and o.postage_stamp_request_id is not null
         and o.id in (select distinct transient_object_id
                        from tcs_transient_reobservations
                       where image_group_id is null)
         and o.object_classification & %s > 0
         order by m.filename, o.followup_id
      """, (detectionList, candidateFlags))
      candidateArray = []
      candidates = cursor.fetchall ()

      for row in candidates:
         candidateArray.append(row["id"])

      cursor.close ()

   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))
      sys.exit (1)

   return candidateArray


# 2010-02-16 KWS Altered query to allow requests to be made for images
#                for previously requested objects that have ongoing
#                recurrent observations.
def searchUniqueObjects(conn, candidate):
   """searchUniqueObjects.

   Args:
       conn:
       candidate:
   """
   import MySQLdb

   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      # The following query should be made a VIEW in the database.
      # This will better facilitate web integration with the Django
      # application.
      # NB The CAST in the statment ensures that trailing zeroes are
      #    added to the result.
      # Added IPP_IDET to list of columns selected
      cursor.execute ("""
         select o.id, o.ipp_idet, o.ra_psf, o.dec_psf, m.imageid, cast(truncate(m.mjd_obs,3) as char) tdate, if(instr(m.filename,'MD'), substr(m.filename, instr(m.filename,'MD'),4), 'null') field, if(instr(m.filename,'skycell'), substr(m.filename, instr(m.filename,'skycell'),instr(m.filename,'.dif') - instr(m.filename,'skycell')), 'null') skycell
         from tcs_transient_objects o, tcs_cmf_metadata m
         where m.id = o.tcs_cmf_metadata_id
         and o.id = %s
         and o.image_group_id is null
         order by m.mjd_obs
      """, (candidate,))
      result_set = cursor.fetchall ()

      cursor.close ()

   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))
      sys.exit (1)


   return result_set

# 2010-02-16 KWS Altered query to allow requests to be made for images
#                for previously requested objects that have ongoing
#                recurrent observations.
# 2010-10-19 KWS Changed query to limit the number of reobseravation images
#                because of load on postage stamp server.  We only need
#                a taste of the images.  This limit can be altered at any
#                time.
def searchRecurrentObjects(conn, candidate, limit = 4): # 2011-08-05 KWS Temporary reduction to 4
   """searchRecurrentObjects.

   Args:
       conn:
       candidate:
       limit:
   """
   import MySQLdb

   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      # The following query should be made a VIEW in the database.
      # This will better facilitate web integration with the Django
      # application.
      # NB The CAST in the statment ensures that trailing zeroes are
      #    added to the result.
      # Added IPP_IDET to list of columns selected
      cursor.execute ("""
         select r.transient_object_id id, r.ipp_idet, r.ra_psf, r.dec_psf, m.imageid, cast(truncate(m.mjd_obs,3) as char) tdate, if(instr(m.filename,'MD'), substr(m.filename, instr(m.filename,'MD'),4), 'null') field, if(instr(m.filename,'skycell'), substr(m.filename, instr(m.filename,'skycell'),instr(m.filename,'.dif') - instr(m.filename,'skycell')), 'null') skycell
         from tcs_transient_reobservations r, tcs_cmf_metadata m
         where m.id = r.tcs_cmf_metadata_id
         and r.transient_object_id = %s
         and r.image_group_id is null
         order by m.mjd_obs desc
         limit %s
      """, (candidate, limit))
      result_set = cursor.fetchall ()

      cursor.close ()

   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))
      sys.exit (1)


   return result_set


def searchUniqueObjectsOverrideExistingImages(conn, candidate):
   """searchUniqueObjectsOverrideExistingImages.

   Args:
       conn:
       candidate:
   """
   import MySQLdb

   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      # The following query should be made a VIEW in the database.
      # This will better facilitate web integration with the Django
      # application.
      # NB The CAST in the statment ensures that trailing zeroes are      #    added to the result.      # Added IPP_IDET to list of columns selected
      cursor.execute ("""
         select o.id, o.ipp_idet, o.ra_psf, o.dec_psf, m.imageid, cast(truncate(m.mjd_obs,3) as char) tdate, if(instr(m.filename,'MD'), substr(m.filename, instr(m.filename,'MD'
),4), 'null') field, if(instr(m.filename,'skycell'), substr(m.filename, instr(m.filename,'skycell'),instr(m.filename,'.dif') - instr(m.filename,'skycell')), 'null') skycell
         from tcs_transient_objects o, tcs_cmf_metadata m
         where m.id = o.tcs_cmf_metadata_id
         and o.id = %s
         order by m.mjd_obs
      """, (candidate,))
      result_set = cursor.fetchall ()

      cursor.close ()

   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))
      sys.exit (1)


   return result_set



def searchRecurrentObjectsOverrideExistingImages(conn, candidate, limit = 6):
   """searchRecurrentObjectsOverrideExistingImages.

   Args:
       conn:
       candidate:
       limit:
   """
   import MySQLdb

   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      # The following query should be made a VIEW in the database.
      # This will better facilitate web integration with the Django
      # application.
      # NB The CAST in the statment ensures that trailing zeroes are
      #    added to the result.
      # Added IPP_IDET to list of columns selected
      cursor.execute ("""
         select r.transient_object_id id, r.ipp_idet, r.ra_psf, r.dec_psf, m.imageid, cast(truncate(m.mjd_obs,3) as char) tdate, if(instr(m.filename,'MD'), substr(m.filename, instr(m.filename,'MD'),4), 'null') field, if(instr(m.filename,'skycell'), substr(m.filename, instr(m.filename,'skycell'),instr(m.filename,'.dif') - instr(m.filename,'skycell')), 'null') skycell
         from tcs_transient_reobservations r, tcs_cmf_metadata m
         where m.id = r.tcs_cmf_metadata_id
         and r.transient_object_id = %s
         order by m.mjd_obs desc
         limit %s
      """, (candidate, limit))
      result_set = cursor.fetchall ()

      cursor.close ()

   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))
      sys.exit (1)


   return result_set


# 2011-09-20 KWS Added new routines to get all occurrences for a finder maker

def searchUniqueObjectsForFinder(conn, candidate, avgRA, avgDEC):
   """searchUniqueObjectsForFinder.

   Args:
       conn:
       candidate:
       avgRA:
       avgDEC:
   """
   import MySQLdb

   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      # The following query should be made a VIEW in the database.
      # This will better facilitate web integration with the Django
      # application.
      # NB The CAST in the statment ensures that trailing zeroes are      #    added to the result.      # Added IPP_IDET to list of columns selected
      cursor.execute ("""
         select o.id, o.ipp_idet, %s ra_psf, %s dec_psf, m.imageid,
             cast(truncate(m.mjd_obs,3) as char) tdate,
             if(instr(m.filename,'MD'), substr(m.filename, instr(m.filename,'MD'),4), 'null') field,
             if(instr(m.filename,'skycell'), substr(m.filename, instr(m.filename,'skycell'),instr(m.filename,'.dif') - instr(m.filename,'skycell')), 'null') skycell,
             if(instr(m.filename,'.V'), substr(m.filename, instr(m.filename,'.V')+1,(instr(m.filename,'.V')+3) - (instr(m.filename,'.V')+1)), 'null') tess_version
         from tcs_transient_objects o, tcs_cmf_metadata m
         where m.id = o.tcs_cmf_metadata_id
         and o.id = %s
         order by m.mjd_obs
      """, (avgRA, avgDEC, candidate))
      result_set = cursor.fetchall ()

      cursor.close ()

   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))
      sys.exit (1)


   return result_set

# 2015-05-31 KWS Updated query to get filename, ppsub_input and ppsub_reference
# 2021-12-28 KWS Get the fpa_detector so we know whether or not to send a gpc1 or gpc2 request.
def searchRecurrentObjectsForFinder(conn, candidate, avgRA, avgDEC, limit = 3):
   """searchRecurrentObjectsForFinder.

   Args:
       conn:
       candidate:
       avgRA:
       avgDEC:
       limit:
   """
   import MySQLdb

   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      cursor.execute ("""
         select r.transient_object_id id, r.ipp_idet, %s ra_psf, %s dec_psf, m.imageid, filename, ppsub_input, ppsub_reference, m.skycell sc, m.fpa_detector,
                cast(truncate(m.mjd_obs,3) as char) tdate,
                if(instr(m.filename,'MD'), substr(m.filename, instr(m.filename,'MD'),4), 'null') field,
                if(instr(m.filename,'skycell'), substr(m.filename, instr(m.filename,'skycell'),instr(m.filename,'.dif') - instr(m.filename,'skycell')), 'null') skycell,
                if(instr(m.filename,'.V'), substr(m.filename, instr(m.filename,'.V')+1,(instr(m.filename,'.V')+3) - (instr(m.filename,'.V')+1)), 'null') tess_version
         from tcs_transient_reobservations r, tcs_cmf_metadata m
         where m.id = r.tcs_cmf_metadata_id
         and r.transient_object_id = %s
         order by m.mjd_obs desc
         limit %s
      """, (avgRA, avgDEC, candidate, limit))
      result_set = cursor.fetchall ()

      cursor.close ()

   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))
      sys.exit (1)


   return result_set




# New function to query the GPC1 diffInputSkyfile table to extract the IDs that we need.
# Probably want to return a dictionary (hash) of some sort...
# We need to return:
#  * type of image (target, reference or difference)
#  * whether or not the target and reference are warps or stacks
#  * the relevant warp or stack IDs
def getGPC1diffInputSkyfile(conn, diffSkyFileId):
   """getGPC1diffInputSkyfile.

   Args:
       conn:
       diffSkyFileId:
   """
   import MySQLdb

   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)
      cursor.execute ("""
         select *
         from gpc1.diffInputSkyfile
         where
         diff_skyfile_id = %s
      """, (diffSkyFileId,))

      if cursor.rowcount > 1:
         print("Warning: More than one row returned.")

      # Only pick up one row, regardless of how many there are
      diffRow = cursor.fetchone ()
      cursor.close ()

   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))
      sys.exit (1)

   return diffRow

# *********************** Detectability Server ************************

# 2020-01-09 KWS Moved the following methods from pstampRequestStamps to here.

# 2013-10-11 KWS Added detections so that we can tie them to existing images
# 2014-07-08 KWS Added extra filter definitions. (Need to fix the query code.)

def getObjectsByList(conn, listId = 4, dateThreshold = '2019-01-01', processingFlags = 0, objectId = None):
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

        if objectId is not None:
            cursor.execute ("""
                select o.id, if(ra_psf < 0, ra_psf + 360.0, ra_psf) ra, dec_psf `dec`, ps1_designation as 'name', followup_flag_date, ps1_designation, object_classification, confidence_factor rb_factor, earliest_mjd
                from tcs_transient_objects o
                left join tcs_latest_object_stats s
                  on s.id = o.id
                where o.id = %s
            """, (objectId,))
            resultSet = cursor.fetchone ()
        else:
            cursor.execute ("""
                select o.id, if(ra_psf < 0, ra_psf + 360.0, ra_psf) ra, dec_psf `dec`, ps1_designation as 'name', followup_flag_date, ps1_designation, object_classification, confidence_factor rb_factor, earliest_mjd
                from tcs_transient_objects o
                left join tcs_latest_object_stats s
                on s.id = o.id
                where detection_list_id = %s
                and (processing_flags & %s = 0 or processing_flags is null)
                and followup_flag_date > %s
                order by followup_id
            """, (listId, processingFlags, dateThreshold))

            resultSet = cursor.fetchall ()
        cursor.close ()

    except MySQLdb.Error as e:
        sys.stderr.write("Error %d: %s\n" % (e.args[0], e.args[1]))
        sys.exit (1)

    return resultSet

# 2023-05-02 KWS Added Pan-STARRS version of getObjectsByCustomList
def getObjectsByCustomList(conn, customList, objectType = -1, processingFlags = PROCESSING_FLAGS['stamps']):
    """getObjectsByCustomList.

    Args:
        conn:
        customList:
        objectType:
        processingFlags:
    """
    import MySQLdb

    try:
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute ("""
            select o.id, o.ra_psf as ra, o.dec_psf as `dec`, o.ps1_designation as 'name', o.followup_flag_date, o.ps1_designation, o.confidence_factor as rb_pix, o.classification_confidence as rb_cat, earliest_mjd
            from tcs_transient_objects o
            join tcs_object_groups g
              on g.transient_object_id = o.id
            left join tcs_latest_object_stats s
              on s.id = o.id
            where g.object_group_id = %s
              and (processing_flags & %s = 0 or processing_flags is null)
            order by o.followup_id
        """, (customList, processingFlags))

        resultSet = cursor.fetchall ()
        cursor.close ()

    except MySQLdb.Error as e:
        print("Error %d: %s" % (e.args[0], e.args[1]))
        sys.exit (1)

    return resultSet


def getATLASObjectsByList(conn, listId = 4, objectType = -1, dateThreshold = '2009-01-01', processingFlags = PROCESSING_FLAGS['stamps'], objectId = None):
    """getObjectsByList.

    Args:
        conn:
        listId:
        objectType:
        dateThreshold:
        processingFlags:
        objectId:
    """
    import MySQLdb

    try:
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)

        if objectId is not None:
            cursor.execute ("""
                select o.id, ifnull(ra_avg, ra) as ra, ifnull(dec_avg, `dec`) as `dec`, o.atlas_designation as 'name', followup_flag_date, atlas_designation, object_classification, zooniverse_score as rb_pix, realbogus_factor, earliest_mjd
                from atlas_diff_objects o
                left join tcs_latest_object_stats s
                  on s.id = o.id
                where o.id = %s
            """, (objectId,))
            resultSet = cursor.fetchone ()
        else:
            cursor.execute ("""
                select o.id, ifnull(ra_avg, ra) as ra, ifnull(dec_avg, `dec`) as `dec`, o.atlas_designation as 'name', followup_flag_date, atlas_designation, other_designation, zooniverse_score as rb_pix, realbogus_factor, earliest_mjd
                from atlas_diff_objects o
                left join tcs_latest_object_stats s
                on s.id = o.id
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


def getATLASObjectsByCustomList(conn, customList, objectType = -1, processingFlags = PROCESSING_FLAGS['stamps']):
    """getObjectsByCustomList.

    Args:
        conn:
        customList:
        objectType:
        processingFlags:
    """
    import MySQLdb

    try:
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute ("""
            select o.id, ifnull(ra_avg, ra) as ra, ifnull(dec_avg, `dec`) as `dec`, o.atlas_designation as 'name', o.followup_flag_date, o.atlas_designation, o.zooniverse_score as rb_pix, o.realbogus_factor, earliest_mjd
            from atlas_diff_objects o
            join tcs_object_groups g
              on g.transient_object_id = o.id
            left join tcs_latest_object_stats s
              on s.id = o.id
            where g.object_group_id = %s
              and (processing_flags & %s = 0 or processing_flags is null)
            order by o.followup_id
        """, (customList, processingFlags))

        resultSet = cursor.fetchall ()
        cursor.close ()

    except MySQLdb.Error as e:
        print("Error %d: %s" % (e.args[0], e.args[1]))
        sys.exit (1)

    return resultSet



def getLightcurveDetections(conn, candidate, filters = "grizywxBV", limit = 0):
   """getLightcurveDetections.

   Args:
       conn:
       candidate:
       filters:
       limit:
   """
   import MySQLdb
   from psat_server_web.ps1.psdb.commonqueries import LC_NON_DET_AND_BLANKS_QUERY, LC_DET_QUERY

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
   """getLightcurveNonDetectionsAndBlanks.

   Args:
       conn:
       candidate:
       filters:
       ippIdetBlank:
   """
   import MySQLdb
   from psat_server_web.ps1.psdb.commonqueries import LC_NON_DET_AND_BLANKS_QUERY, LC_DET_QUERY

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
    """getExistingNonDetectionImages.

    Args:
        conn:
        candidate:
        ippIdetBlank:
    """
    import MySQLdb

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
    """getExistingDetectionImages.

    Args:
        conn:
        candidate:
        ippIdetBlank:
    """
    import MySQLdb

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


# Get the AVERAGE RA and DEC for the candidate

def getAverageCoordinates(conn, candidate, mjd = None):
   """getAverageCoordinates.

   Args:
       conn:
       candidate:
       mjd:
   """
   import MySQLdb
   ra = None
   dec = None

   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      if mjd:
         cursor.execute ("""
            select avg(ra_psf) ra_psf, avg(dec_psf) dec_psf from (
               select ra_psf, dec_psf
                 from tcs_transient_objects o, tcs_cmf_metadata m
                where o.id = %s
                  and o.tcs_cmf_metadata_id = m.id
                  and m.mjd_obs > %s
            union all
               select r.ra_psf, r.dec_psf
                 from tcs_transient_reobservations r, tcs_transient_objects o, tcs_cmf_metadata m
                where o.id = r.transient_object_id
                  and o.id = %s
                  and r.tcs_cmf_metadata_id = m.id
                  and m.mjd_obs > %s ) temp
         """, (candidate, mjd, candidate, mjd))
      else:
         cursor.execute ("""
            select avg(ra_psf) ra_psf, avg(dec_psf) dec_psf from (
               select ra_psf, dec_psf
                 from tcs_transient_objects
                where id = %s
            union all
               select r.ra_psf, r.dec_psf
                 from tcs_transient_reobservations r, tcs_transient_objects o
                where o.id = r.transient_object_id
                  and o.id = %s) temp
         """, (candidate, candidate))

      if cursor.rowcount > 1:
         print("Warning: More than one row returned.")

      # Only pick up one row, regardless of how many there are
      row = cursor.fetchone ()
      cursor.close ()

   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))
      sys.exit (1)

   if row:
      ra = row["ra_psf"]
      dec = row["dec_psf"]

   return ra, dec



# Get ALL filters and (float) MJDs in which an object may be positioned.
# To do this, we need to grab all the skycells in which the objects is located.
# Query is a bit messy and can probably be done more efficiently in parts

# Note that I'll probably use this to extract missing postage stamps as well,
# hence the extraction of the diffSkyFileId (imageid), field and skycell.

def getAllDetectionInfo(conn, candidate):
   """getAllDetectionInfo.

   Args:
       conn:
       candidate:
   """
   import MySQLdb
   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      # Note that the literal percent sign must be escaped with another percent sign
      cursor.execute ("""
         select distinct imageid,
                         if(instr(mm.filename,'MD'), substr(mm.filename, instr(mm.filename,'MD'),4), 'null') field,
                         if(instr(mm.filename,'skycell'), replace(substr(mm.filename, instr(mm.filename,'skycell'),instr(mm.filename,'.dif') - instr(mm.filename,'skycell')), '.SS', ''), 'null') skycell,
                         substr(mm.fpa_filterid,1,1) filter,
                         mjd_obs
           from tcs_cmf_metadata mm,
           (
             select distinct field, skycell
               from (
               select if(instr(m.filename,'MD'), substr(m.filename, instr(m.filename,'MD'),4), 'null') field,
                      if(instr(m.filename,'skycell'), replace(substr(m.filename, instr(m.filename,'skycell'),instr(m.filename,'.dif') - instr(m.filename,'skycell')), '.SS', ''), 'null') skycell
                 from tcs_cmf_metadata m, tcs_transient_objects o
                where o.tcs_cmf_metadata_id = m.id
                  and o.id = %s
                union
               select if(instr(m.filename,'MD'), substr(m.filename, instr(m.filename,'MD'),4), 'null') field,
                      if(instr(m.filename,'skycell'), replace(substr(m.filename, instr(m.filename,'skycell'),instr(m.filename,'.dif') - instr(m.filename,'skycell')), '.SS', ''), 'null') skycell
                 from tcs_cmf_metadata m, tcs_transient_reobservations r
                where r.tcs_cmf_metadata_id = m.id
                  and r.transient_object_id = %s
               ) fieldandskycell
           ) det
         where mm.filename like concat(det.field, '%%', det.skycell, '%%')
         order by mm.mjd_obs
      """, (candidate, candidate))
      resultSet = cursor.fetchall ()

      cursor.close ()

   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))
      sys.exit (1)


   return resultSet


# Get ALL filters and (whole) MJDs in which an object may be positioned.
# To do this, we need to grab all the skycells in which the objects is located.
# Query is a bit messy and can probably be done more efficiently in parts

# Note that the detectability server current insists on WHOLE integer MJDs.

# 2013-02-27 KWS Forced the query to return only ONE row per filter per day by just grabbing
#                integer MJDs.

def getDetectabilityInfo(conn, candidate):
   """getDetectabilityInfo.

   Args:
       conn:
       candidate:
   """
   import MySQLdb
   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      # Note that the literal percent sign must be escaped with another percent sign
      # 2013-07-17 KWS Rewritten query to use SQL CASE statements to make it compatible with
      #                both 3pi and MD.
      cursor.execute ("""
         select distinct substr(mm.fpa_filterid,1,1) filter,
                         truncate(mjd_obs, 0) mjd_obs
           from tcs_cmf_metadata mm,
           (
             select distinct field, skycell
               from (
               select
                      case
                          when instr(m.filename,'MD') then substr(m.filename, instr(m.filename,'MD'),4)
                          when instr(m.filename,'RINGS') then substr(m.filename, instr(m.filename,'RINGS'),8)
                          else 'null'
                      end as field,
                      case
                          -- 3pi
                          when instr(m.filename,'WS') then if(instr(m.filename,'skycell'), replace(substr(m.filename, instr(m.filename,'skycell'),instr(m.filename,'.dif') - instr(m.filename,'skycell')), '.WS', ''), 'null')
                          -- MD
                          else if(instr(m.filename,'skycell'), replace(substr(m.filename, instr(m.filename,'skycell'),instr(m.filename,'.dif') - instr(m.filename,'skycell')), '.SS', ''), 'null')
                      end as skycell
                 from tcs_cmf_metadata m, tcs_transient_objects o
                where o.tcs_cmf_metadata_id = m.id
                  and o.id = %s
                union
               select
                      case
                          when instr(m.filename,'MD') then substr(m.filename, instr(m.filename,'MD'),4)
                          when instr(m.filename,'RINGS') then substr(m.filename, instr(m.filename,'RINGS'),8)
                          else 'null'
                      end as field,
                      case
                          -- 3pi
                          when instr(m.filename,'WS') then if(instr(m.filename,'skycell'), replace(substr(m.filename, instr(m.filename,'skycell'),instr(m.filename,'.dif') - instr(m.filename,'skycell')), '.WS', ''), 'null')
                          -- MD
                          else if(instr(m.filename,'skycell'), replace(substr(m.filename, instr(m.filename,'skycell'),instr(m.filename,'.dif') - instr(m.filename,'skycell')), '.SS', ''), 'null')
                      end as skycell
                 from tcs_cmf_metadata m, tcs_transient_reobservations r
                where r.tcs_cmf_metadata_id = m.id
                  and r.transient_object_id = %s
               ) fieldandskycell
           ) det
         where mm.filename like concat(det.field, '%%', det.skycell, '%%')
         order by mm.mjd_obs
      """, (candidate, candidate))
      resultSet = cursor.fetchall ()
      print("TEST")

      cursor.close ()

   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))
      sys.exit (1)


   return resultSet

def getMaxForcedPhotometryMJD(conn, candidate):
    """getMaxForcedPhotometryMJD.

    Args:
        conn:
        candidate:
    """
    import MySQLdb

    try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      cursor.execute ("""
          select ifnull(max(mjd_obs),0) maxmjd
            from tcs_forced_photometry
           where transient_object_id = %s
      """, (candidate,))
      resultSet = cursor.fetchone ()

      cursor.close ()

    except MySQLdb.Error as e:
        print("Error %d: %s" % (e.args[0], e.args[1]))
        sys.exit (1)

    return resultSet['maxmjd']


# 2020-01-09 KWS Completely rewrote detectability info request
def getDetectabilityInfo2(conn, candidate, limitDays = 100, limitDaysAfter = 0, useFirstDetection = True):
    """getDetectabilityInfo2.

    Args:
        conn:
        candidate:
        limitDays:
        limitDaysAfter:
        useFirstDetection:
    """

    import json
    detectabilityData = []
    lightcurveData = getLightcurveDetections(conn, candidate)
    lightcurveData += getLightcurveNonDetectionsAndBlanks(conn, candidate)

    lightcurveData = sorted(lightcurveData, key = lambda i: i['tdate'])
    # NOTE: there are duplicates in the non-forced photometry with different values of ipp_idet but all
    #       the rest are the same.  We should eliminate the duplicates below before sending to the
    #       detectability server, otherwise it will not produce the correct results.

    if limitDays > 0:
        thresholdMJDMax = 70000
        if useFirstDetection:
            # We need to know when the first detection was,
            # but we don't always request detections.
            detectionData = getLightcurveDetections(conn, candidate)
            # The detection MJD should be the first element returned
            thresholdMJD = detectionData[0]['mjd'] - limitDays
            if limitDaysAfter > 0:
                thresholdMJDMax = detectionData[0]['mjd'] + limitDaysAfter
        else:
            thresholdMJD = getCurrentMJD() - limitDays
        lightcurveData = eliminateOldDetections(conn, candidate, lightcurveData, thresholdMJD, thresholdMJDMax)

    # If we already have forced photometry, don't request it again.
    maxForcedPhotometryMJD = getMaxForcedPhotometryMJD(conn, candidate)

    if lightcurveData:
        for row in lightcurveData:
            diffImageCombination = findIdCombinationForPostageStampRequest2(row)
            # We just need the diff ID
            # Don't request forced photometry that we already have!
            if row['mjd'] > maxForcedPhotometryMJD:
                print(row['id'], row['fpa_detector'])
                for imType in ['target','ref','diff']:
                    print("%s (%s): %s" % (imType, diffImageCombination[imType][0], diffImageCombination[imType][1]), end=' ')
                print()
                detectabilityData.append(json.dumps({'mjd': row['mjd'], 'filter': row['filter'], 'diff': diffImageCombination['diff'][1], 'target': diffImageCombination['target'][1], 'pscamera': row['fpa_detector']}))

        # Now we must eliminate the dupes.  Sadly, we can't use sets, since "dicts are not hashable" but we
        # can do a trick by converting the dict to json and then back to dict.
        if len(detectabilityData) > 0:
            detectabilityDataJSON = list(set(detectabilityData))
            detectabilityData = []
            for d in detectabilityDataJSON:
                detectabilityData.append(json.loads(d))
            # Finally sort them the list again.  (Set leaves it unsorted once again.)
            detectabilityData = sorted(detectabilityData, key = lambda i: i['mjd'])

    return detectabilityData

def detectabilityCandidateList(conn, candidateFlags = -1, detectionList = 4):
   """detectabilityCandidateList.

   Args:
       conn:
       candidateFlags:
       detectionList:
   """
   import MySQLdb
   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      cursor.execute ("""
         select o.id from tcs_transient_objects o, tcs_cmf_metadata m
         where o.tcs_cmf_metadata_id = m.id
         and o.followup_id is not null
         and o.detection_list_id = %s
         and o.detection_list_id != 0
         order by o.followup_id
      """, (detectionList,))
      candidateArray = []
      candidates = cursor.fetchall ()

      for row in candidates:
         candidateArray.append(row["id"])

      cursor.close ()

   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))
      sys.exit (1)

   return candidateArray


def addForcedPhotRow(conn, candidate, ra, dec, targetFlux, targetFluxSig, mjdObs, filter, detectN = 0, detectF = 0, errorCode = 0, fpaId = 'unknown', postageStampRequestId = 0, calPsfMag = None, calPsfMagSig = None, exptime = None):
   """addForcedPhotRow.

   Args:
       conn:
       candidate:
       ra:
       dec:
       targetFlux:
       targetFluxSig:
       mjdObs:
       filter:
       detectN:
       detectF:
       errorCode:
       fpaId:
       postageStampRequestId:
       calPsfMag:
       calPsfMagSig:
       exptime:
   """
   import MySQLdb
   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      cursor.execute ("""
          insert into tcs_forced_photometry (transient_object_id, ra, decl, mjd_obs, filter, error_code, detect_n, detect_f, target_flux, target_flux_sig, fpa_id, postage_stamp_request_id, cal_psf_mag, cal_psf_mag_sig, exptime)
          values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
          """, (candidate, ra, dec, mjdObs, filter, errorCode, detectN, detectF, targetFlux, targetFluxSig, fpaId, postageStampRequestId, calPsfMag, calPsfMagSig, exptime))

   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))

   cursor.close ()
   return conn.insert_id()


def deleteForcedPhotometry(conn, candidate):
   """deleteForcedPhotometry.

   Args:
       conn:
       candidate:
   """
   import MySQLdb

   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)
      cursor.execute ("""
          delete from tcs_forced_photometry
          where transient_object_id = %s
          """, (candidate,))

      cursor.close ()

   except MySQLdb.Error as e:
      if e[0] == 1142: # Can't delete - don't have permission
         print("Can't delete.  User doesn't have permission.")
      else:
         print(e)

   return conn.affected_rows()

# 2021-12-28 KWS Added pscamera header - so we now know which telescope this came from. 
def insertForcedPhotometry(conn, row):
   """insertForcedPhotometry.

   Args:
       conn:
       row:
   """
   import MySQLdb

   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      cursor.execute ("""
          insert into tcs_forced_photometry
               (transient_object_id,
                mjd_obs,
                exptime,
                filter,
                zero_pt,
                postage_stamp_request_id,
                rownum,
                skycell,
                fpa_id,
                x_psf,
                y_psf,
                x_psf_sig,
                y_psf_sig,
                posangle,
                pltscale,
                psf_inst_mag,
                psf_inst_mag_sig,
                psf_inst_flux,
                psf_inst_flux_sig,
                ap_mag,
                ap_mag_radius,
                peak_flux_as_mag,
                cal_psf_mag,
                cal_psf_mag_sig,
                ra_psf,
                dec_psf,
                sky,
                sky_sigma,
                psf_chisq,
                cr_nsigma,
                ext_nsigma,
                psf_major,
                psf_minor,
                psf_theta,
                psf_qf,
                psf_ndof,
                psf_npix,
                moments_xx,
                moments_xy,
                moments_yy,
                diff_npos,
                diff_fratio,
                diff_nratio_bad,
                diff_nratio_mask,
                diff_nratio_all,
                flags,
                n_frames,
                padding,
                pscamera)
          values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
          """, (row["transient_object_id"],
                row["mjd_obs"],
                row["exptime"],
                row["filter"],
                row["zero_pt"],
                row["postage_stamp_request_id"],
                row["rownum"],
                row["skycell"],
                row["fpa_id"],
                row["x_psf"],
                row["y_psf"],
                row["x_psf_sig"],
                row["y_psf_sig"],
                row["posangle"],
                row["pltscale"],
                row["psf_inst_mag"],
                row["psf_inst_mag_sig"],
                row["psf_inst_flux"],
                row["psf_inst_flux_sig"],
                row["ap_mag"],
                row["ap_mag_radius"],
                row["peak_flux_as_mag"],
                row["cal_psf_mag"],
                row["cal_psf_mag_sig"],
                row["ra_psf"],
                row["dec_psf"],
                row["sky"],
                row["sky_sigma"],
                row["psf_chisq"],
                row["cr_nsigma"],
                row["ext_nsigma"],
                row["psf_major"],
                row["psf_minor"],
                row["psf_theta"],
                row["psf_qf"],
                row["psf_ndof"],
                row["psf_npix"],
                row["moments_xx"],
                row["moments_xy"],
                row["moments_yy"],
                row["diff_npos"],
                row["diff_fratio"],
                row["diff_nratio_bad"],
                row["diff_nratio_mask"],
                row["diff_nratio_all"],
                row["flags"],
                row["n_frames"],
                row["padding"],
                row["pscamera"]))

   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))

   cursor.close ()
   return conn.insert_id()


# 2013-07-05 KWS Added code to update the tcs_images table with the most recent
#                row of images.
# 2013-09-23 KWS Make sure we are referring to the most recent *detection*, not non-detection.

def getRecentImageGroupRow(conn, objectId, ippIdet = IPP_IDET_NON_DETECTION_VALUE):
    """
    Grab the most recent image group. Grab the first row of the sorted list descending,
    because name contains the id and the MJD.
    """
    import MySQLdb

    # 2014-03-08 KWS Modified this code to select only image groups
    #                with a group type of null. This allows us to use
    #                group type for Finder images.
    try:
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute ("""
            select id, name
              from tcs_image_groups
             where name like '%s%%'
               and name not like '%s%%\_%s'
               and group_type is null
          order by name desc
             limit 1
        """, (objectId, objectId, ippIdet))

        resultSet = cursor.fetchone ()
        cursor.close ()

    except MySQLdb.Error as e:
        print("Error %d: %s" % (e.args[0], e.args[1]))
        sys.exit (1)

    return resultSet


def getPostageStampsForImageGroup(conn, imageGroupId):
    """
    Grab the (three) rows relating to the max group id for this object
    """
    import MySQLdb

    try:
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute ("""
            select *
              from tcs_postage_stamp_images
             where image_group_id = %s
        """, (imageGroupId,))

        resultSet = cursor.fetchall ()
        cursor.close ()

    except MySQLdb.Error as e:
        print("Error %d: %s" % (e.args[0], e.args[1]))
        sys.exit (1)

    return resultSet



def insertNewImageTripletReference(conn, objectId, imageData):
    """
    Insert the chosen image triplet into the tcs_images table and return its ID
    """
    import MySQLdb

    try:
        cursor = conn.cursor (MySQLdb.cursors.DictCursor)

        # Delete any existing data relating to this object
        cursor.execute("""
            delete from tcs_images
             where target like '%s%%'
        """, (objectId,))

        cursor.execute ("""
            insert into tcs_images (target, ref, diff, mjd_obs)
            values (%s, %s, %s, %s)
          """, (imageData["target"], imageData["ref"], imageData["diff"], imageData["mjd_obs"]))

    except MySQLdb.Error as e:
        print("Error %d: %s" % (e.args[0], e.args[1]))

    cursor.close ()
    return conn.insert_id()




def updateObjectImageTripletReference(conn, objectId, imageId):
    """
    Update the tcs_images_id reference to refer to the relevant image triplet
    """
    import MySQLdb

    rowsUpdated = 0
    try:
        cursor = conn.cursor (MySQLdb.cursors.DictCursor)

        cursor.execute ("""
           update tcs_transient_objects
              set tcs_images_id = %s
            where id = %s
              """, (imageId, objectId))


    except MySQLdb.Error as e:
        print("Error %d: %s" % (e.args[0], e.args[1]))

    rowsUpdated = cursor.rowcount
    cursor.close ()

    return rowsUpdated






# FGSS queries
# We need to pick up the WARP from the GPC1 database.  In the first instance, I'm going
# to attempt to find the MAX warp ID.

def searchFGSSUniqueObjects(conn, candidate):
   """searchFGSSUniqueObjects.

   Args:
       conn:
       candidate:
   """
   import MySQLdb

   # MOTE: in this query we DON'T select the max warp ID.  This could lead to problems if the data is
   #       reprocessed.  The query would need to be redesigned though.
   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      cursor.execute ("""
        select o.id, o.ipp_idet, ra_psf, dec_psf, warp_id, imageid, cast(truncate(m.mjd_obs,3) as char) tdate
          from gpc1.warpRun
          join gpc1.fakeRun
         using (fake_id)
          join gpc1.camRun
         using (cam_id)
          join gpc1.camProcessedExp
         using (cam_id)
          join gpc1.chipRun
         using (chip_id)
          join gpc1.rawExp
         using (exp_id)
          join tcs_cmf_metadata m
            on (exp_id = m.imageid)
          join tcs_transient_objects o
            on (o.tcs_cmf_metadata_id = m.id)
         where o.id = %s
      """, (candidate,))
      result_set = cursor.fetchall ()

      cursor.close ()

   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))
      sys.exit (1)


   return result_set


def searchFGSSRecurrentObjects(conn, candidate, limit = 6):
   """searchFGSSRecurrentObjects.

   Args:
       conn:
       candidate:
       limit:
   """
   import MySQLdb

   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      cursor.execute ("""
        select r.transient_object_id id, r.ipp_idet, ra_psf, dec_psf, warp_id, imageid, cast(truncate(m.mjd_obs,3) as char) tdate
          from gpc1.warpRun
          join gpc1.fakeRun
         using (fake_id)
          join gpc1.camRun
         using (cam_id)
          join gpc1.camProcessedExp
         using (cam_id)
          join gpc1.chipRun
         using (chip_id)
          join gpc1.rawExp
         using (exp_id)
          join tcs_cmf_metadata m
            on (exp_id = m.imageid)
          join tcs_transient_reobservations r
            on (r.tcs_cmf_metadata_id = m.id)
         where r.transient_object_id = %s
         limit %s
      """, (candidate, limit))
      result_set = cursor.fetchall ()

      cursor.close ()

   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))
      sys.exit (1)


   return result_set



# 2012-04-03 KWS New FGSS queries to talk to the REMOTE GPC1 database ONLY.
#                Need to join the resultsets manually because MySQL doesn't
#                do remote database joins.

def getFGSSUniqueObjects(conn, candidate):
   """getFGSSUniqueObjects.

   Args:
       conn:
       candidate:
   """
   import MySQLdb

   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      cursor.execute ("""
        select o.id, o.ipp_idet, ra_psf, dec_psf, imageid, cast(truncate(m.mjd_obs,3) as char) tdate
          from tcs_transient_objects o
          join tcs_cmf_metadata m
            on (o.tcs_cmf_metadata_id = m.id)
         where o.id = %s
      """, (candidate,))
      result_set = cursor.fetchall ()
      cursor.close ()

   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))
      sys.exit (1)


   return result_set


def getFGSSRecurrentObjects(conn, candidate, limit = 6):
   """getFGSSRecurrentObjects.

   Args:
       conn:
       candidate:
       limit:
   """
   import MySQLdb

   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      # FIRST - Create a set with everything except the warp_id

      cursor.execute ("""
        select r.transient_object_id id, r.ipp_idet, r.ra_psf, r.dec_psf, imageid, cast(truncate(m.mjd_obs,3) as char) tdate
          from tcs_transient_reobservations r
          join tcs_cmf_metadata m
            on (r.tcs_cmf_metadata_id = m.id)
         where r.transient_object_id = %s
          limit %s
      """, (candidate, limit))
      result_set = cursor.fetchall ()
      cursor.close ()

   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))
      sys.exit (1)


   return result_set


def getFGSSWarpId(conn, exposureId):
   """getFGSSWarpId.

   Args:
       conn:
       exposureId:
   """
   import MySQLdb

   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      cursor.execute ("""
        select max(warp_id) warp_id
          from gpc1.warpRun
          join gpc1.fakeRun
         using (fake_id)
          join gpc1.camRun
         using (cam_id)
          join gpc1.camProcessedExp
         using (cam_id)
          join gpc1.chipRun
         using (chip_id)
          join gpc1.rawExp
         using (exp_id)
         where exp_id = %s
      """, (exposureId,))
      result_set = cursor.fetchone ()

      cursor.close ()

   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))
      sys.exit (1)


   return result_set



def searchFGSSUniqueObjectsRemote(conn, connRemote, candidate):
   """searchFGSSUniqueObjectsRemote.

   Args:
       conn:
       connRemote:
       candidate:
   """

   # FIRST - Create a set with everything except the warp_id

   resultSet = getFGSSUniqueObjects(conn, candidate)

   # THEN - Add in the warp IDs manually.  The resultset is a list of dicts, so we can
   #        force a new dict entry for each row for 'warp_id'.

   for row in resultSet:
      warpIdResult = getFGSSWarpId(connRemote, row['imageid'])
      if warpIdResult:
         row['warp_id'] = warpIdResult['warp_id']
      else:
         print("Something went wrong.  Can't find warp ID.")

   return resultSet


def searchFGSSRecurrentObjectsRemote(conn, connRemote, candidate, limit = 6):
   """searchFGSSRecurrentObjectsRemote.

   Args:
       conn:
       connRemote:
       candidate:
       limit:
   """

   # FIRST - Create a set with everything except the warp_id

   resultSet = getFGSSRecurrentObjects(conn, candidate, limit = limit)

   # THEN - Add in the warp IDs manually.  The resultset is a list of dicts, so we can
   #        force a new dict entry for each row for 'warp_id'.

   for row in resultSet:
      warpIdResult = getFGSSWarpId(connRemote, row['imageid'])
      if warpIdResult:
         row['warp_id'] = warpIdResult['warp_id']
      else:
         print("Something went wrong.  Can't find warp ID.")

   return resultSet



# 2011-06-13 KWS Added new function to get ALL IMAGEID values for a named MD field and a list of skycells.
#                NOTE: that this query is formed differently from the usual mechanism, because we want to
#                create an IN list from a list of skycells.

def getInfoForNamedSkycells(conn, field, skycells):
   """getInfoForNamedSkycells.

   Args:
       conn:
       field:
       skycells:
   """
   import MySQLdb

   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)


      sqlSelect = '''
               select m.imageid, m.fpa_filter,
                      cast(truncate(m.mjd_obs,3) as char) tdate,
                      if(instr(m.filename,'MD'), substr(m.filename, instr(m.filename,'MD'),4), 'null') field,
                      if(instr(m.filename,'skycell'), substr(m.filename, instr(m.filename,'skycell')+length('skycell')+1,3), 'null') skycell
               from tcs_cmf_metadata m
              where substr(m.filename, instr(m.filename,'MD'),4) = '%s'
      ''' % field
   
      sqlWhere = '''
                and (
                     substr(m.filename, instr(m.filename,'skycell')+length('skycell')+1,3) IN (%s)
                    )
               order by m.mjd_obs desc
      '''
   
      in_p=', '.join(['%s' for x in skycells])
   
      sql = sqlSelect + sqlWhere % in_p
   
      cursor.execute(sql, skycells)

      result_set = cursor.fetchall ()


   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))
      sys.exit (1)


   return result_set





def findIdCombinationForPostageStampRequest(diffRow):
   """findIdCombinationForPostageStampRequest.

   Args:
       diffRow:
   """

   warpStackCombination = None
   diffImageCombination = {}
   # Only 4 states are allowed for the warpStackCombination value

   # warpStackCombination
   # wsws
   # 1122
   # ----
   # 0000
   # 0001
   # 0010
   # 0011
   # 0100
   # 0101 (5) Stack-Stack diffs -> New for MD surverys
   # 0110 (6) Stack-Warp diffs -> Reverse of current default MD survery
   # 0111
   # 1000
   # 1001 (9) Warp-Stack diffs -> Current default for MD survey
   # 1010 (10) Warp-Warp diffs -> Current default for 3pi survey
   # 1011
   # 1100
   # 1101
   # 1110
   # 1111

   warpStackCombination = int(diffRow["warp1"] is not None) * 8 + int(diffRow["stack1"] is not None) * 4 + int(diffRow["warp2"] is not None) * 2 + int(diffRow["stack2"] is not None)

   if warpStackCombination not in [5, 6, 9, 10]:
      print("Illegal combination of values from diffInputSkyfile table.")
      return diffImageCombination

   # OK - Now we need to set the values for target, ref, diff

   if warpStackCombination == 5:
      diffImageCombination = {'target': ('stack', diffRow["stack1"]),
                                 'ref': ('stack', diffRow["stack2"]),
                                'diff': ('diff', diffRow["diff_id"])}
   elif warpStackCombination == 6:
      diffImageCombination = {'target': ('stack', diffRow["stack1"]),
                                 'ref': ('warp', diffRow["warp1"]),
                                'diff': ('diff', diffRow["diff_id"])}
   elif warpStackCombination == 9:
      diffImageCombination = {'target': ('warp', diffRow["warp1"]),
                                 'ref': ('stack', diffRow["stack2"]),
                                'diff': ('diff', diffRow["diff_id"])}
   elif warpStackCombination == 10:
      diffImageCombination = {'target': ('warp', diffRow["warp1"]),
                                 'ref': ('warp', diffRow["warp2"]),
                                'diff': ('diff', diffRow["diff_id"])}

   # Test - let's print out the values:
   for key, value in list(diffImageCombination.items()):
      print("%s %s %d" % (key, value[0], value[1]))

   return diffImageCombination


# 2015-05-31 KWS The diff image info is now stored in the header. We no longer need
#                to poll the GPC1 database.

diffRegex = 'dif\.([0-9]+)\.'
diffRegexCompiled = re.compile(diffRegex)
refRegex = '(wrp|stk)\.([0-9]+)\.'
refRegexCompiled = re.compile(refRegex)
inputRegex = '(wrp|stk)\.([0-9]+)\.'
inputRegexCompiled = re.compile(inputRegex)

def findIdCombinationForPostageStampRequest2(cmfInfo):
    """findIdCombinationForPostageStampRequest2.

    Args:
        cmfInfo:
    """

    diffImageCombination = {}

    warpStack = {'wrp': 'warp', 'stk': 'stack'}

    input = ref = diff = None
    inputType = refType = None

    # A diff is always a diff, and the diff ID always comes after dif.XXXXXX
    # However, for the inputs we need to determine whether or not they are stack
    # or warps.  Likewise for the references (usually stacks, but never assume).

    s = inputRegexCompiled.search(cmfInfo['ppsub_input'])
    if s:
        inputType = s.group(1)
        input = s.group(2)
    s = refRegexCompiled.search(cmfInfo['ppsub_reference'])
    if s:
        refType = s.group(1)
        ref = s.group(2)
    s = diffRegexCompiled.search(cmfInfo['filename'])
    if s:
        diff = s.group(1)

    if input and ref and diff:
        diffImageCombination = {'target': (warpStack[inputType], input),
                                'ref': (warpStack[refType], ref),
                                'diff': ('diff', diff)}

    return diffImageCombination


# Detectability Request regular expressions to dig out exposure name. This
# ONLY works with input warps.  If we need to do stacks - no idea how this
# will work.
inputWarpRegex = '^(o[0-9][0-9][0-9][0-9]g[0-9][0-9][0-9][0-9]o)\.'
inputWarpRegexCompiled = re.compile(inputWarpRegex)

def findExpNameCombinationForDetectabilityRequest(cmfInfo):
    """findExpNameCombinationForDetectabilityRequest.

    Args:
        cmfInfo:
    """

    input = None

    # A diff is always a diff, and the diff ID always comes after dif.XXXXXX
    # However, for the inputs we need to determine whether or not they are stack
    # or warps.  Likewise for the references (usually stacks, but never assume).

    s = inputWarpRegexCompiled.search(cmfInfo['ppsub_input'])
    if s:
        input = s.group(1)

    return input


# We need to change this function.  Currently I assume warp, stack, diff.  I should probably specify
# target, ref, diff instead, and deduce what they should be at the PS Request building stage.
# Need to re-think how "imageTypes" is used. See suggested replacement function below.

# *** THIS FUNCTION NO LONGER USED ***
def writeFITSRequest(outfile, requestName, results, imageTypes, xsize, ysize, psRequestType = 'bydiff'):
   """writeFITSRequest.

   Args:
       outfile:
       requestName:
       results:
       imageTypes:
       xsize:
       ysize:
       psRequestType:
   """

   hdu = pf.PrimaryHDU()
   hdulist = pf.HDUList()
   prihdr = hdu.header

   # Make the following changes so that the primary header comments are
   # IDENTICAL to example Postage Stamp Server requests.

   prihdr.set('SIMPLE', True, 'file does conform to FITS standard')
   prihdr.set('BITPIX', 16, comment='number of bits per data pixel')
   prihdr.set('NAXIS', 0, comment='number of data axes')
   prihdr.set('EXTEND', True, 'FITS dataset may contain extensions')
   prihdr.add_comment("  FITS (Flexible Image Transport System) format is defined in 'Astronomy")
   prihdr.add_comment("  and Astrophysics', volume 376, page 359; bibcode: 2001A&A...376..359H")
   hdulist.append(hdu)

   # There's probably a more elegant way to do this!!
   rownum = []
   center_x = [] # RA
   center_y = [] # DEC
   width = []
   height = []
   coord_mask = []
   job_type = []
   option_mask = []
   project = []
   req_type = []
   img_type = []
   id = []
   tess_id = []
   component = []
   label = []
   reqfilt = []
   mjd_min = []
   mjd_max = []
   comment = []

   row = 1

   # 2012-09-21 KWS Discovered that PyFITS3 doesn't allow implicit creation of
   #                double arrays from integer lists.  Need to cast integers
   #                as floats.
   for result in results:
      for imageType in imageTypes:
         rownum.append(row)
         center_x.append(result["ra_psf"])
         center_y.append(result["dec_psf"])
         width.append(float(xsize))
         height.append(float(ysize))
         coord_mask.append(2)
         job_type.append('stamp')
         option_mask.append(1)
         project.append('gpc1')
         req_type.append(psRequestType)
         img_type.append(imageType)
         id.append(result["imageid"])
         tess_id.append(result["field"])
         component.append(result["skycell"])
         label.append('null')
         reqfilt.append('null')
         mjd_min.append(0)
         mjd_max.append(0)
         # Added IPP_IDET to list of columns selected
         comment.append('%d_%s_%s_%d_%s' % (result["id"], result["tdate"], result["imageid"], result["ipp_idet"], imageType))
         row = row + 1

   # Create the FITS columns.

   rownum_col = pf.Column(name='ROWNUM', format='J', array=rownum)
   center_x_col = pf.Column(name='CENTER_X', format='D', array=center_x)
   center_y_col = pf.Column(name='CENTER_Y', format='D', array=center_y)
   width_col = pf.Column(name='WIDTH', format='D', array=width)
   height_col = pf.Column(name='HEIGHT', format='D', array=height)
   coord_mask_col = pf.Column(name='COORD_MASK', format='J', array=coord_mask)
   job_type_col = pf.Column(name='JOB_TYPE', format='16A', array=job_type)
   option_mask_col = pf.Column(name='OPTION_MASK', format='J', array=option_mask)
   project_col = pf.Column(name='PROJECT', format='16A', array=project)
   req_type_col = pf.Column(name='REQ_TYPE', format='16A', array=req_type)
   img_type_col = pf.Column(name='IMG_TYPE', format='16A', array=img_type)
   id_col = pf.Column(name='ID', format='16A', array=id)
   tess_id_col = pf.Column(name='TESS_ID', format='64A', array=tess_id)
   component_col = pf.Column(name='COMPONENT', format='64A', array=component)
   label_col = pf.Column(name='LABEL', format='64A', array=label)
   reqfilt_col = pf.Column(name='REQFILT', format='16A', array=reqfilt)
   mjd_min_col = pf.Column(name='MJD_MIN', format='D', array=mjd_min)
   mjd_max_col = pf.Column(name='MJD_MAX', format='D', array=mjd_max)
   comment_col = pf.Column(name='COMMENT', format='64A', array=comment)

   cols=pf.ColDefs([rownum_col,center_x_col,center_y_col,width_col,height_col,coord_mask_col,job_type_col,option_mask_col,project_col,req_type_col,img_type_col,id_col,tess_id_col,component_col,label_col,reqfilt_col,mjd_min_col,mjd_max_col,comment_col])

   tbhdu=pf.BinTableHDU.from_columns(cols)

   hdulist.append(tbhdu)
   exthdr = hdulist[1].header

   # The Postage Stamp Server is very choosy about our files.  Thus,
   # I have added identical column comments to the example files.  Note
   # that PyFITS doesn't seem to support the creation of comments at
   # column creation!  I'll fix this when I find out.


   exthdr.set('EXTNAME','PS1_PS_REQUEST','name of this binary table extension')
   exthdr.set('REQ_NAME',requestName,'Postage Stamp request name')
   exthdr.set('EXTVER','1','Extension version')
   hdulist.writeto(outfile, overwrite=True)

   return


# 2010-02-08 KWS New Function.  Eventually I hope this will replace the original function above...
#                Needs to talk to the GPC1 database to pick up the relevant image IDs.

# 2015-03-11 KWS Added the gpc1 connection to the method signature.  If we can't get a connection,
#                no point continuing.
# 2015-03-25 KWS Modified the main stamp request code to use V2 of the stamp request format

def writeFITSPostageStampRequestById(conn, gpc1Conn, outfile, requestName, results, xsize, ysize, psRequestType = 'byid', optionMask = 2049, email = 'qub2@qub.ac.uk', camera = 'gpc1'):
   """writeFITSPostageStampRequestById.

   Args:
       conn:
       gpc1Conn:
       outfile:
       requestName:
       results:
       xsize:
       ysize:
       psRequestType:
       optionMask:
       email:
       camera:
   """

   # In the new function we're going to request the images 'byid' rather than 'bydiff'.  This
   # requires a query to be made against the local GPC1 database.

   # From the query, we need to get warp1 OR stack1 and warp2 OR stack2.  The choices are:
   # warp1, stack2 (the current default stack-to-warp differences)
   # stack1, stack2 (defining the stack-to-stack differences)
   # warp1, warp2 (the 3pi warp-to-warp differences)
   # stack1, warp2 (a warp-to-stack difference where the reference image is actually the warp, not the stack)


   # First query the GPC 1 database.  If we can't do that, we may as well abort...

   fileSuccessfullyWritten = False



   hdu = pf.PrimaryHDU()
   hdulist = pf.HDUList()
   prihdr = hdu.header

   # Make the following changes so that the primary header comments are
   # IDENTICAL to example Postage Stamp Server requests.

   prihdr.set('SIMPLE', True, 'file does conform to FITS standard')
   prihdr.set('BITPIX', 16, comment='number of bits per data pixel')
   prihdr.set('NAXIS', 0, comment='number of data axes')
   prihdr.set('EXTEND', True, 'FITS dataset may contain extensions')
   prihdr.add_comment("  FITS (Flexible Image Transport System) format is defined in 'Astronomy")
   prihdr.add_comment("  and Astrophysics', volume 376, page 359; bibcode: 2001A&A...376..359H")
   hdulist.append(hdu)

   # There's probably a more elegant way to do this!!
   rownum = []
   project = []
   survey_name = []
   ipp_release = []
   job_type = []
   option_mask = []
   req_type = []
   img_type = []
   id = []
   tess_id = []
   component = []
   coord_mask = []
   center_x = [] # RA
   center_y = [] # DEC
   width = []
   height = []
   #label = []
   data_group = []
   reqfilt = []
   mjd_min = []
   mjd_max = []
   run_type = []
   fwhm_min = []
   fwhm_max = []
   comment = []

   row = 1

   # 2012-01-09 KWS Make remote connection to GPC1 DB in Hawaii.
   #                If it fails use the local GPC1 DB.

   for result in results:

      #if gpc1Conn:
      #   gpc1DiffInputSkyfileRow = getGPC1diffInputSkyfile(gpc1Conn, result["imageid"])
      #else:
      #   #print "No remote database available.  Connecting to local database."
      #   #gpc1DiffInputSkyfileRow = getGPC1diffInputSkyfile(conn, result["imageid"])
      #   print "No remote database available.  Cannot connect to local DB for the time being, so skipping this row.."
      #   continue

      #if not gpc1DiffInputSkyfileRow:
      #   print "No matching rows for diffInputSkyfile %s. Skipping to the next row." % result["imageid"]
      #   continue
 
      # 2015-05-31 KWS We can now bypass GPC1 database and get the image info
      #                from the CMF header.
      diffImageCombination = findIdCombinationForPostageStampRequest2(result)

      if not diffImageCombination:
         print("No valid image combination for diffInputSkyfile" % result["imageid"])
         continue

      # 2010-02-21 KWS Populate skycell ID from GPC1 database row rather than filename, which
      #                is now broken because of the 'WS' or 'SS' component in the filename.
      # 2012-09-21 KWS Discovered that PyFITS3 doesn't allow implicit creation of
      #                double arrays from integer lists.  Need to cast integers
      #                as floats.
      for imageType, value in list(diffImageCombination.items()):
         rownum.append(row)

         # 2020-06-07 KWS TEMPORARY FIX: If the image type is 'ref' force camera to be gpc1.
         try:
             cam = result["fpa_detector"].lower()
         except KeyError as e:
             cam = camera

         if imageType == 'ref':
             cam = 'gpc1'

         project.append(cam)
         survey_name.append('null')
         ipp_release.append('null')
         job_type.append('stamp')
         option_mask.append(optionMask)  # Changed to 2049 for unconvolved stacks
         req_type.append(psRequestType)
         img_type.append(value[0])
         id.append(value[1])
         tess_id.append(result["field"])
         component.append('skycell.'  + str(result["sc"])) # Changed skycell to the one extracted from CMF header
         coord_mask.append(2)
         center_x.append(result["ra_psf"])
         center_y.append(result["dec_psf"])
         width.append(float(xsize))
         height.append(float(ysize))
         #label.append('null')
         data_group.append('null')
         reqfilt.append('null')
         mjd_min.append(0)
         mjd_max.append(0)
         run_type.append('null')
         fwhm_min.append(0)
         fwhm_max.append(0)
         # Added IPP_IDET to list of columns selected
         comment.append('%d_%s_%s_%d_%s' % (result["id"], result["tdate"], result["imageid"], result["ipp_idet"], imageType))
         row = row + 1

   # Create the FITS columns.

   rownum_col = pf.Column(name='ROWNUM', format='J', array=rownum)
   project_col = pf.Column(name='PROJECT', format='16A', array=project)
   survey_name_col = pf.Column(name='SURVEY_NAME', format='16A', array=survey_name)
   ipp_release_col = pf.Column(name='IPP_RELEASE', format='16A', array=ipp_release)
   job_type_col = pf.Column(name='JOB_TYPE', format='16A', array=job_type)

   option_mask_col = pf.Column(name='OPTION_MASK', format='J', array=option_mask)

   req_type_col = pf.Column(name='REQ_TYPE', format='16A', array=req_type)
   img_type_col = pf.Column(name='IMG_TYPE', format='16A', array=img_type)
   id_col = pf.Column(name='ID', format='16A', array=id)
   tess_id_col = pf.Column(name='TESS_ID', format='64A', array=tess_id)
   component_col = pf.Column(name='COMPONENT', format='64A', array=component)

   coord_mask_col = pf.Column(name='COORD_MASK', format='J', array=coord_mask)
   center_x_col = pf.Column(name='CENTER_X', format='D', array=center_x)
   center_y_col = pf.Column(name='CENTER_Y', format='D', array=center_y)
   width_col = pf.Column(name='WIDTH', format='D', array=width)
   height_col = pf.Column(name='HEIGHT', format='D', array=height)

   #label_col = pf.Column(name='LABEL', format='64A', array=label)
   data_group_col = pf.Column(name='DATA_GROUP', format='64A', array=data_group)
   reqfilt_col = pf.Column(name='REQFILT', format='16A', array=reqfilt)
   mjd_min_col = pf.Column(name='MJD_MIN', format='D', array=mjd_min)
   mjd_max_col = pf.Column(name='MJD_MAX', format='D', array=mjd_max)
   run_type_col = pf.Column(name='RUN_TYPE', format='16A', array=run_type)
   fwhm_min_col = pf.Column(name='FWHM_MIN', format='D', array=fwhm_min)
   fwhm_max_col = pf.Column(name='FWHM_MAX', format='D', array=fwhm_max)
   comment_col = pf.Column(name='COMMENT', format='64A', array=comment)

   cols=pf.ColDefs([rownum_col,
                    project_col,
                    survey_name_col,
                    ipp_release_col,
                    job_type_col,
                    option_mask_col,
                    req_type_col,
                    img_type_col,
                    id_col,
                    tess_id_col,
                    component_col,
                    coord_mask_col,
                    center_x_col,
                    center_y_col,
                    width_col,
                    height_col,
                    data_group_col,
                    reqfilt_col,
                    mjd_min_col,
                    mjd_max_col,
                    run_type_col,
                    fwhm_min_col,
                    fwhm_max_col,
                    comment_col])

   tbhdu=pf.BinTableHDU.from_columns(cols)

   hdulist.append(tbhdu)
   exthdr = hdulist[1].header

   exthdr.set('EXTNAME','PS1_PS_REQUEST','name of this binary table extension')
   exthdr.set('REQ_NAME',requestName,'Postage Stamp request name')

   # 2015-03-24 KWS Updated contents of the header for version 2
   exthdr.set('EXTVER','2','Extension version')
   exthdr.set('ACTION','PROCESS')
   exthdr.set('EMAIL',email,'Email address of submitter')
   hdulist.writeto(outfile, overwrite=True)

   fileSuccessfullyWritten = True

   return fileSuccessfullyWritten



# 2011-06-12 KWS New Function - Extract same info as above but this time we only know the
#                RA, DEC and skycell.

# 2015-03-11 KWS Added the gpc1 connection to the method signature.  If we can't get a connection,
#                no point continuing.

def writeFITSPostageStampRequestBySkycell(conn, gpc1Conn, outfile, requestName, ra, dec, results, xsize, ysize, psRequestType = 'byid', optionMask = 2049):
   """writeFITSPostageStampRequestBySkycell.

   Args:
       conn:
       gpc1Conn:
       outfile:
       requestName:
       ra:
       dec:
       results:
       xsize:
       ysize:
       psRequestType:
       optionMask:
   """

   # From the query, we need to get warp1 OR stack1 and warp2 OR stack2.  The choices are:
   # warp1, stack2 (the current default stack-to-warp differences)
   # stack1, stack2 (defining the stack-to-stack differences)
   # warp1, warp2 (the 3pi warp-to-warp differences)
   # stack1, warp2 (a warp-to-stack difference where the reference image is actually the warp, not the stack)

   fileSuccessfullyWritten = False

   hdu = pf.PrimaryHDU()
   hdulist = pf.HDUList()
   prihdr = hdu.header

   # Make the following changes so that the primary header comments are
   # IDENTICAL to example Postage Stamp Server requests.

   prihdr.set('SIMPLE', True, 'file does conform to FITS standard')
   prihdr.set('BITPIX', 16, comment='number of bits per data pixel')
   prihdr.set('NAXIS', 0, comment='number of data axes')
   prihdr.set('EXTEND', True, 'FITS dataset may contain extensions')
   prihdr.add_comment("  FITS (Flexible Image Transport System) format is defined in 'Astronomy")
   prihdr.add_comment("  and Astrophysics', volume 376, page 359; bibcode: 2001A&A...376..359H")
   hdulist.append(hdu)

   # There's probably a more elegant way to do this!!
   rownum = []
   center_x = [] # RA
   center_y = [] # DEC
   width = []
   height = []
   coord_mask = []
   job_type = []
   option_mask = []
   project = []
   req_type = []
   img_type = []
   id = []
   tess_id = []
   component = []
   label = []
   reqfilt = []
   mjd_min = []
   mjd_max = []
   comment = []

   row = 1

   # 2012-01-09 KWS Make remote connection to GPC1 DB in Hawaii.
   #                If it fails use the local GPC1 DB.

   for result in results:

      if gpc1Conn:
         gpc1DiffInputSkyfileRow = getGPC1diffInputSkyfile(gpc1Conn, result["imageid"])
      else:
         #gpc1DiffInputSkyfileRow = getGPC1diffInputSkyfile(conn, result["imageid"])
         print("No remote DB available.  Skipping this row...")
         continue

      if not gpc1DiffInputSkyfileRow:
         print("No matching rows for diffInputSkyfile %s. Skipping to the next row." % result["imageid"])
         continue
 
      diffImageCombination = findIdCombinationForPostageStampRequest(gpc1DiffInputSkyfileRow)

      if not diffImageCombination:
         print("No valid image combination for diffInputSkyfile" % result["imageid"])
         continue

      # 2010-02-21 KWS Populate skycell ID from GPC1 database row rather than filename, which
      #                is now broken because of the 'WS' or 'SS' component in the filename.
      # 2012-09-21 KWS Discovered that PyFITS3 doesn't allow implicit creation of
      #                double arrays from integer lists.  Need to cast integers
      #                as floats.
      for imageType, value in list(diffImageCombination.items()):
         rownum.append(row)
         center_x.append(ra)
         center_y.append(dec)
         width.append(float(xsize))
         height.append(float(ysize))
         coord_mask.append(2)
         job_type.append('stamp')
         option_mask.append(optionMask)  # Changed to 2049 for unconvolved stacks
         project.append('gpc1')
         req_type.append(psRequestType)
         img_type.append(value[0])
         id.append(value[1])
         tess_id.append(result["field"])
         component.append(gpc1DiffInputSkyfileRow["skycell_id"])
         label.append('null')
         reqfilt.append('null')
         mjd_min.append(0)
         mjd_max.append(0)
         # Added IPP_IDET to list of columns selected
         comment.append('%s_%d_%s' % (result["tdate"], result["imageid"], imageType))
         row = row + 1


   # Create the FITS columns.

   rownum_col = pf.Column(name='ROWNUM', format='J', array=rownum)
   center_x_col = pf.Column(name='CENTER_X', format='D', array=center_x)
   center_y_col = pf.Column(name='CENTER_Y', format='D', array=center_y)
   width_col = pf.Column(name='WIDTH', format='D', array=width)
   height_col = pf.Column(name='HEIGHT', format='D', array=height)
   coord_mask_col = pf.Column(name='COORD_MASK', format='J', array=coord_mask)
   job_type_col = pf.Column(name='JOB_TYPE', format='16A', array=job_type)
   option_mask_col = pf.Column(name='OPTION_MASK', format='J', array=option_mask)
   project_col = pf.Column(name='PROJECT', format='16A', array=project)
   req_type_col = pf.Column(name='REQ_TYPE', format='16A', array=req_type)
   img_type_col = pf.Column(name='IMG_TYPE', format='16A', array=img_type)
   id_col = pf.Column(name='ID', format='16A', array=id)
   tess_id_col = pf.Column(name='TESS_ID', format='64A', array=tess_id)
   component_col = pf.Column(name='COMPONENT', format='64A', array=component)
   label_col = pf.Column(name='LABEL', format='64A', array=label)
   reqfilt_col = pf.Column(name='REQFILT', format='16A', array=reqfilt)
   mjd_min_col = pf.Column(name='MJD_MIN', format='D', array=mjd_min)
   mjd_max_col = pf.Column(name='MJD_MAX', format='D', array=mjd_max)
   comment_col = pf.Column(name='COMMENT', format='64A', array=comment)

   cols=pf.ColDefs([rownum_col,center_x_col,center_y_col,width_col,height_col,coord_mask_col,job_type_col,option_mask_col,project_col,req_type_col,img_type_col,id_col,tess_id_col,component_col,label_col,reqfilt_col,mjd_min_col,mjd_max_col,comment_col])

   tbhdu=pf.BinTableHDU.from_columns(cols)

   hdulist.append(tbhdu)
   exthdr = hdulist[1].header

   # The Postage Stamp Server is very choosy about our files.  Thus,
   # I have added identical column comments to the example files.  Note
   # that PyFITS doesn't seem to support the creation of comments at
   # column creation!  I'll fix this when I find out.


   exthdr.set('EXTNAME','PS1_PS_REQUEST','name of this binary table extension')
   exthdr.set('REQ_NAME',requestName,'Postage Stamp request name')
   exthdr.set('EXTVER','1','Extension version')
   hdulist.writeto(outfile, overwrite=True)

   fileSuccessfullyWritten = True

   return fileSuccessfullyWritten






# ****************** Detectability Request ********************

# 2020-05-01 KWS Added PROJECT card, which must be set to whichever camera is being used.
# 2022-10-12 KWS Added coords. The object we are interested in might actually be NEAR but
#                not exactly the one we want the photometry for. So override the coordinates
#                and get the forced photometry with position set as the one of interest.
#                Use the nearby object as a proxy to request and store the forced photometry. 
#                We found the nearby object by cone searching around the object of interest.
def writeDetectabilityFITSRequest(conn, outfile, requestName, candidateList, diffType = 'WSdiff', email = 'qub2@qub.ac.uk', camera = 'gpc1', limitDays = 100, limitDaysAfter = 0, coords = []):
   """writeDetectabilityFITSRequest.

   Args:
       conn:
       outfile:
       requestName:
       candidateList:
       diffType:
       email:
       camera:
       limitDays:
       limitDaysAfter:
   """

   fileSuccessfullyWritten = False

   hdu = pf.PrimaryHDU()
   hdulist = pf.HDUList()
   prihdr = hdu.header

   # Make the following changes so that the primary header comments are
   # IDENTICAL to example Postage Stamp Server requests.

   prihdr.set('SIMPLE', True, 'file does conform to FITS standard')
   prihdr.set('BITPIX', 16, comment='number of bits per data pixel')
   prihdr.set('NAXIS', 0, comment='number of data axes')
   prihdr.set('EXTEND', True, 'FITS dataset may contain extensions')
   prihdr.add_comment("  FITS (Flexible Image Transport System) format is defined in 'Astronomy")
   prihdr.add_comment("  and Astrophysics', volume 376, page 359; bibcode: 2001A&A...376..359H")
   hdulist.append(hdu)

   rownum = []
   project = []
   ra1_deg = []
   dec1_deg = []
   ra2_deg = []
   dec2_deg = []
   mjd_obs = []
   filter = []
   fpa_id = []

   row = 1

   for candidate in candidateList:
      # Find the average RA/DEC for the candidate
      if coords:
          (ra, dec) = (float(coords[0]), float(coords[1]))
      else:
          (ra, dec) = getAverageCoordinates(conn, candidate['id'])

      # Pick up the detectability info for this candidate
      detectabilityResultSet = getDetectabilityInfo2(conn, candidate['id'], limitDays = limitDays, limitDaysAfter = limitDaysAfter)
      if len(detectabilityResultSet) == 0:
          # Do NOT write any requests
          print("No data to request!")
          continue

      # 2012-04-16 KWS Use base 26 of candidate ID to reduce size of column.
      #                Will need to create code to restore candidate ID from
      #                base 26 version.
      # 2012-09-21 KWS Discovered that PyFITS3 doesn't allow implicit creation of
      #                double arrays from integer lists.  Need to cast integers
      #                as floats.
      for result in detectabilityResultSet:
         #rownum.append(str(candidate)) # Detectability rows are ASCII values
         rownum.append("%s_%05d" % (base26(candidate['id']), row))
         try:
             project.append(result["pscamera"].lower())
         except KeyError as e:
             project.append(camera)
         #rownum.append("%05d" % (row))
         ra1_deg.append(ra)
         dec1_deg.append(dec)
         ra2_deg.append(ra)
         dec2_deg.append(dec)
         mjd_obs.append(float(int(result["mjd"])))
         filter.append(result["filter"])
         if diffType == 'warp' or diffType == 'stack':
             fpa_id.append(int(result["target"]))
         else:
             fpa_id.append(int(result["diff"]))
         row = row + 1

   # Create the FITS columns.
   if row == 1:
       # No candidates were added to the table.  Don't send an empty request!
       return fileSuccessfullyWritten

   rownum_col = pf.Column(name='ROWNUM', format='20A', array=rownum)
   project_col = pf.Column(name='PROJECT', format='16A', array=project)
   ra1_deg_col = pf.Column(name='RA1_DEG', format='D', array=ra1_deg)
   dec1_deg_col = pf.Column(name='DEC1_DEG', format='D', array=dec1_deg)
   ra2_deg_col = pf.Column(name='RA2_DEG', format='D', array=ra2_deg)
   dec2_deg_col = pf.Column(name='DEC2_DEG', format='D', array=dec2_deg)
   filter_col = pf.Column(name='FILTER', format='20A', array=filter)
   mjd_obs_col = pf.Column(name='MJD-OBS', format='D', array=mjd_obs)
   fpa_id_col = pf.Column(name='FPA_ID', format='J', array=fpa_id)

   cols=pf.ColDefs([rownum_col,project_col,ra1_deg_col,dec1_deg_col,ra2_deg_col,dec2_deg_col,filter_col,mjd_obs_col,fpa_id_col])

   tbhdu=pf.BinTableHDU.from_columns(cols)

   hdulist.append(tbhdu)
   exthdr = hdulist[1].header

   exthdr.set('EXTNAME','MOPS_DETECTABILITY_QUERY','name of this binary table extension')
   exthdr.set('QUERY_ID',requestName,'MOPS Query ID for this batch query')
   exthdr.set('EXTVER','2','Extension version')
   exthdr.set('OBSCODE','566','site identifier (MPC observatory code)')
   exthdr.set('STAGE',diffType,'processing stage to examine')
   exthdr.set('EMAIL',email,'Email address of submitter')
   hdulist.writeto(outfile, overwrite=True)

   fileSuccessfullyWritten = True

   return fileSuccessfullyWritten



def writeDetectabilityManualFITSRequest(outfile, requestName, ra, dec, epochRows, diffType = 'SSdiff', email = 'qub2@qub.ac.uk'):
   """writeDetectabilityManualFITSRequest.

   Args:
       outfile:
       requestName:
       ra:
       dec:
       epochRows:
       diffType:
       email:
   """

   fileSuccessfullyWritten = False

   hdu = pf.PrimaryHDU()
   hdulist = pf.HDUList()
   prihdr = hdu.header

   # Make the following changes so that the primary header comments are
   # IDENTICAL to example Postage Stamp Server requests.

   prihdr.set('SIMPLE', True, 'file does conform to FITS standard')
   prihdr.set('BITPIX', 16, comment='number of bits per data pixel')
   prihdr.set('NAXIS', 0, comment='number of data axes')
   prihdr.set('EXTEND', True, 'FITS dataset may contain extensions')
   prihdr.add_comment("  FITS (Flexible Image Transport System) format is defined in 'Astronomy")
   prihdr.add_comment("  and Astrophysics', volume 376, page 359; bibcode: 2001A&A...376..359H")
   hdulist.append(hdu)

   rownum = []
   ra1_deg = []
   dec1_deg = []
   ra2_deg = []
   dec2_deg = []
   mag = []
   mjd_obs = []
   filter = []

   rownumber = 1


   for row in epochRows:
      rownum.append("%05d" % (rownumber))
      ra1_deg.append(ra)
      dec1_deg.append(dec)
      ra2_deg.append(ra)
      dec2_deg.append(dec)
      mag.append(19) # Not used
      mjd_obs.append(float(int(row["mjd_obs"])))
      filter.append(row["filter"])
      rownumber = rownumber + 1

   # Create the FITS columns.

   rownum_col = pf.Column(name='ROWNUM', format='20A', array=rownum)
   ra1_deg_col = pf.Column(name='RA1_DEG', format='D', array=ra1_deg)
   dec1_deg_col = pf.Column(name='DEC1_DEG', format='D', array=dec1_deg)
   ra2_deg_col = pf.Column(name='RA2_DEG', format='D', array=ra2_deg)
   dec2_deg_col = pf.Column(name='DEC2_DEG', format='D', array=dec2_deg)
   mag_col = pf.Column(name='MAG', format='D', array=mag)
   mjd_obs_col = pf.Column(name='MJD-OBS', format='D', array=mjd_obs)
   filter_col = pf.Column(name='FILTER', format='20A', array=filter)

   cols=pf.ColDefs([rownum_col,ra1_deg_col,dec1_deg_col,ra2_deg_col,dec2_deg_col,mag_col,mjd_obs_col,filter_col])

   tbhdu=pf.BinTableHDU.from_columns(cols)

   hdulist.append(tbhdu)
   exthdr = hdulist[1].header

   exthdr.set('EXTNAME','MOPS_DETECTABILITY_QUERY','name of this binary table extension')
   exthdr.set('QUERY_ID',requestName,'MOPS Query ID for this batch query')
   exthdr.set('EXTVER','2','Extension version')
   exthdr.set('OBSCODE','566','site identifier (MPC observatory code)')
   exthdr.set('STAGE',diffType,'processing stage to examine')
   exthdr.set('EMAIL',email,'Email address of submitter')
   hdulist.writeto(outfile, overwrite=True)

   fileSuccessfullyWritten = True

   return fileSuccessfullyWritten


# 2016-01-11 KWS New type of detectability using input exposure ID (FPA_ID).  This time
#                the MJD-OBS will be the *exact* MJD-OBS of the given exposure. Likewise
#                for the FILTER. The input data can be aquired using the code to grab
#                the detection and non-detection lightcurve data. If not available (e.g.
#                there isn't a detection at that point) then inject the exposures manually.
#                Dave's code can grab the required exposure CMF data.
def writeDetectabilityFITSRequestByExpName(conn, outfile, requestName, candidateList, diffType = 'WSdiff', email = 'qub2@qub.ac.uk'):
   """writeDetectabilityFITSRequestByExpName.

   Args:
       conn:
       outfile:
       requestName:
       candidateList:
       diffType:
       email:
   """

   fileSuccessfullyWritten = False

   hdu = pf.PrimaryHDU()
   hdulist = pf.HDUList()
   prihdr = hdu.header

   # Make the following changes so that the primary header comments are
   # IDENTICAL to example Postage Stamp Server requests.

   prihdr.set('SIMPLE', True, 'file does conform to FITS standard')
   prihdr.set('BITPIX', 16, comment='number of bits per data pixel')
   prihdr.set('NAXIS', 0, comment='number of data axes')
   prihdr.set('EXTEND', True, 'FITS dataset may contain extensions')
   prihdr.add_comment("  FITS (Flexible Image Transport System) format is defined in 'Astronomy")
   prihdr.add_comment("  and Astrophysics', volume 376, page 359; bibcode: 2001A&A...376..359H")
   hdulist.append(hdu)

   rownum = []
   ra1_deg = []
   dec1_deg = []
   ra2_deg = []
   dec2_deg = []
   mag = []
   mjd_obs = []
   fpa_id = []
   filter = []

   row = 1

   for candidate in candidateList:
         #rownum.append(str(candidate)) # Detectability rows are ASCII values
         rownum.append("%s_%05d" % (candidate['id'], row))
         #rownum.append("%05d" % (row))
         ra1_deg.append(float(candidate['ra']))
         dec1_deg.append(float(candidate['dec']))
         ra2_deg.append(float(candidate['ra']))
         dec2_deg.append(float(candidate['dec']))
         mag.append(candidate['mag']) # Not used
         mjd_obs.append(float(candidate['mjd']))
         fpa_id.append(candidate['fpa_id'])
         filter.append(candidate['filter'])
         row = row + 1

   # Create the FITS columns.

   rownum_col = pf.Column(name='ROWNUM', format='20A', array=rownum)
   ra1_deg_col = pf.Column(name='RA1_DEG', format='D', array=ra1_deg)
   dec1_deg_col = pf.Column(name='DEC1_DEG', format='D', array=dec1_deg)
   ra2_deg_col = pf.Column(name='RA2_DEG', format='D', array=ra2_deg)
   dec2_deg_col = pf.Column(name='DEC2_DEG', format='D', array=dec2_deg)
   mag_col = pf.Column(name='MAG', format='D', array=mag)
   mjd_obs_col = pf.Column(name='MJD-OBS', format='D', array=mjd_obs)
   fpa_id_col = pf.Column(name='FPA_ID', format='20A', array=fpa_id)
   filter_col = pf.Column(name='FILTER', format='20A', array=filter)

   #cols=pf.ColDefs([rownum_col,ra1_deg_col,dec1_deg_col,ra2_deg_col,dec2_deg_col,mag_col,filter_col,mjd_obs_col,fpa_id_col])
   # 2016-02-05 KWS Removed the mjd column
   cols=pf.ColDefs([rownum_col,ra1_deg_col,dec1_deg_col,ra2_deg_col,dec2_deg_col,mag_col,filter_col,fpa_id_col])

   tbhdu=pf.BinTableHDU.from_columns(cols)

   hdulist.append(tbhdu)
   exthdr = hdulist[1].header

   exthdr.set('EXTNAME','MOPS_DETECTABILITY_QUERY','name of this binary table extension')
   exthdr.set('QUERY_ID',requestName,'MOPS Query ID for this batch query')
   exthdr.set('EXTVER','2','Extension version')
   exthdr.set('OBSCODE','566','site identifier (MPC observatory code)')
   exthdr.set('STAGE',diffType,'processing stage to examine')
   exthdr.set('EMAIL',email,'Email address of submitter')
   hdulist.writeto(outfile, overwrite=True)

   fileSuccessfullyWritten = True

   return fileSuccessfullyWritten



# ****************** Warp Stage image requests for FGSS *******************

# 2015-08-26 KWS This code is used by the MANUAL stamp requester and therefore
#                required updating to V2 format.
# 2020-06-27 KWS Can now pass coordMask.  Values are:
#                0 center in RA/DEC; width & height in arc seconds
#                1 center in pixel coordinates; width and height in arc seconds
#                2 center in RA/DEC; width and height in pixels
#                3 center x/y, width, and height in pixel coordinates
def writeFGSSPostageStampRequestById(outfile, requestName, results, xsize, ysize, psRequestType = 'byid', optionMask = 2049, imageType = 'warp', psJobType = 'stamp', skycell = 'null', email = 'qub2@qub.ac.uk', camera = 'gpc1', coordMask = 2):
   """writeFGSSPostageStampRequestById.

   Args:
       outfile:
       requestName:
       results:
       xsize:
       ysize:
       psRequestType:
       optionMask:
       imageType:
       psJobType:
       skycell:
       email:
       camera:
       coordMask:
   """

   # "results" is the data set returned from the database of all the candidates.  Need
   # to construct a suitable query that contains the appropriate columns.
   fileSuccessfullyWritten = False

   hdu = pf.PrimaryHDU()
   hdulist = pf.HDUList()
   prihdr = hdu.header

   prihdr.set('SIMPLE', True, 'file does conform to FITS standard')
   prihdr.set('BITPIX', 16, comment='number of bits per data pixel')
   prihdr.set('NAXIS', 0, comment='number of data axes')
   prihdr.set('EXTEND', True, 'FITS dataset may contain extensions')
   prihdr.add_comment("  FITS (Flexible Image Transport System) format is defined in 'Astronomy")
   prihdr.add_comment("  and Astrophysics', volume 376, page 359; bibcode: 2001A&A...376..359H")
   hdulist.append(hdu)

   rownum = []
   project = []
   survey_name = []
   ipp_release = []
   job_type = []
   option_mask = []
   req_type = []
   img_type = []
   id = []
   tess_id = []
   component = []
   coord_mask = []
   center_x = [] # RA
   center_y = [] # DEC
   width = []
   height = []
   #label = []
   data_group = []
   reqfilt = []
   mjd_min = []
   mjd_max = []
   run_type = []
   fwhm_min = []
   fwhm_max = []
   comment = []

   row = 1

   # 2012-09-21 KWS Discovered that PyFITS3 doesn't allow implicit creation of
   #                double arrays from integer lists.  Need to cast integers
   #                as floats.
   for result in results:
      rownum.append(row)
      project.append(camera)
      survey_name.append('null')
      ipp_release.append('null')
      job_type.append('stamp')
      option_mask.append(optionMask)  # Changed to 2049 for unconvolved stacks
      req_type.append(psRequestType)
      img_type.append(imageType)  # Hard wired to warp for FGSS 3pi data
      id.append(result["warp_id"]) # This should contain the warp ID as extracted from the GPC1 database
      tess_id.append('RINGS.V3')
      component.append(skycell)
      coord_mask.append(coordMask)
      center_x.append(float(result["ra_psf"]))
      center_y.append(float(result["dec_psf"]))
      width.append(float(xsize))
      height.append(float(ysize))
      #label.append('null')
      data_group.append('null')
      reqfilt.append('null')
      mjd_min.append(0)
      mjd_max.append(0)
      run_type.append('null')
      fwhm_min.append(0)
      fwhm_max.append(0)
      # Added IPP_IDET to list of columns selected
      try:
          if result["comment"]:
              comment.append(result["comment"])
          else:
              comment.append('%s_%s_%s_%d_%s' % (str(result["id"]), result["tdate"], result["imageid"], result["ipp_idet"], "target")) # Hard wired "target" as image type
      except KeyError as e:
          comment.append('%s_%s_%s_%d_%s' % (str(result["id"]), result["tdate"], result["imageid"], result["ipp_idet"], "target")) # Hard wired "target" as image type
      row = row + 1

   # Create the FITS columns.

   rownum_col = pf.Column(name='ROWNUM', format='J', array=rownum)
   project_col = pf.Column(name='PROJECT', format='16A', array=project)
   survey_name_col = pf.Column(name='SURVEY_NAME', format='16A', array=survey_name)
   ipp_release_col = pf.Column(name='IPP_RELEASE', format='16A', array=ipp_release)
   job_type_col = pf.Column(name='JOB_TYPE', format='16A', array=job_type)

   option_mask_col = pf.Column(name='OPTION_MASK', format='J', array=option_mask)

   req_type_col = pf.Column(name='REQ_TYPE', format='16A', array=req_type)
   img_type_col = pf.Column(name='IMG_TYPE', format='16A', array=img_type)
   id_col = pf.Column(name='ID', format='16A', array=id)
   tess_id_col = pf.Column(name='TESS_ID', format='64A', array=tess_id)
   component_col = pf.Column(name='COMPONENT', format='64A', array=component)

   coord_mask_col = pf.Column(name='COORD_MASK', format='J', array=coord_mask)
   center_x_col = pf.Column(name='CENTER_X', format='D', array=center_x)
   center_y_col = pf.Column(name='CENTER_Y', format='D', array=center_y)
   width_col = pf.Column(name='WIDTH', format='D', array=width)
   height_col = pf.Column(name='HEIGHT', format='D', array=height)

   #label_col = pf.Column(name='LABEL', format='64A', array=label)
   data_group_col = pf.Column(name='DATA_GROUP', format='64A', array=data_group)
   reqfilt_col = pf.Column(name='REQFILT', format='16A', array=reqfilt)
   mjd_min_col = pf.Column(name='MJD_MIN', format='D', array=mjd_min)
   mjd_max_col = pf.Column(name='MJD_MAX', format='D', array=mjd_max)
   run_type_col = pf.Column(name='RUN_TYPE', format='16A', array=run_type)
   fwhm_min_col = pf.Column(name='FWHM_MIN', format='D', array=fwhm_min)
   fwhm_max_col = pf.Column(name='FWHM_MAX', format='D', array=fwhm_max)
   comment_col = pf.Column(name='COMMENT', format='64A', array=comment)

   cols=pf.ColDefs([rownum_col,
                    project_col,
                    survey_name_col,
                    ipp_release_col,
                    job_type_col,
                    option_mask_col,
                    req_type_col,
                    img_type_col,
                    id_col,
                    tess_id_col,
                    component_col,
                    coord_mask_col,
                    center_x_col,
                    center_y_col,
                    width_col,
                    height_col,
                    data_group_col,
                    reqfilt_col,
                    mjd_min_col,
                    mjd_max_col,
                    run_type_col,
                    fwhm_min_col,
                    fwhm_max_col,
                    comment_col])

   tbhdu=pf.BinTableHDU.from_columns(cols)
   # The from_columns method only available from PyFITS 3.3 onwards.
   #tbhdu=pf.BinTableHDU.from_columns(cols)

   hdulist.append(tbhdu)
   exthdr = hdulist[1].header

   exthdr.set('EXTNAME','PS1_PS_REQUEST','name of this binary table extension')
   exthdr.set('REQ_NAME',requestName,'Postage Stamp request name')

   # 2015-08-26 KWS Updated contents of the header for version 2
   exthdr.set('EXTVER','2','Extension version')
   exthdr.set('ACTION','PROCESS')
   exthdr.set('EMAIL',email,'Email address of submitter')
   hdulist.writeto(outfile, overwrite=True)

   fileSuccessfullyWritten = True

   return fileSuccessfullyWritten


# 2014-01-30 KWS Altered imageType to represent 'target', 'ref' or 'diff'.
#                By default we get the target, but we can override with the ref
#                to get a better looking finder.

# 2015-03-11 KWS Added the gpc1 connection to the method signature.  If we can't get a connection,
#                no point continuing.
# 2015-03-27 KWS Modified the main stamp request code to use V2 of the stamp request format
# 2015-05-31 KWS Modified code to ignore the GPC1 database

def writeFinderPostageStampRequestById(conn, gpc1Conn, outfile, requestName, results, sizeInArcsec, psRequestType = 'byid', optionMask = 2049, imageType = 'target', email = 'qub2@qub.ac.uk', camera = 'gpc1'):
   """writeFinderPostageStampRequestById.

   Args:
       conn:
       gpc1Conn:
       outfile:
       requestName:
       results:
       sizeInArcsec:
       psRequestType:
       optionMask:
       imageType:
       email:
       camera:
   """

   # "results" is the data set returned from the database of all the candidates.  Need
   # to construct a suitable query that contains the appropriate columns.
   fileSuccessfullyWritten = False

   hdu = pf.PrimaryHDU()
   hdulist = pf.HDUList()
   prihdr = hdu.header

   prihdr.set('SIMPLE', True, 'file does conform to FITS standard')
   prihdr.set('BITPIX', 16, comment='number of bits per data pixel')
   prihdr.set('NAXIS', 0, comment='number of data axes')
   prihdr.set('EXTEND', True, 'FITS dataset may contain extensions')
   prihdr.add_comment("  FITS (Flexible Image Transport System) format is defined in 'Astronomy")
   prihdr.add_comment("  and Astrophysics', volume 376, page 359; bibcode: 2001A&A...376..359H")
   hdulist.append(hdu)

   rownum = []
   project = []
   survey_name = []
   ipp_release = []
   job_type = []
   option_mask = []
   req_type = []
   img_type = []
   id = []
   tess_id = []
   component = []
   coord_mask = []
   center_x = [] # RA
   center_y = [] # DEC
   width = []
   height = []
   #label = []
   data_group = []
   reqfilt = []
   mjd_min = []
   mjd_max = []
   run_type = []
   fwhm_min = []
   fwhm_max = []
   comment = []

   row = 1

   for result in results:
      if result["tess_version"] == 'V3':
         xsize = int(sizeInArcsec/0.25)
         ysize = int(sizeInArcsec/0.25)
      else:
         xsize = int(sizeInArcsec/0.2)
         ysize = int(sizeInArcsec/0.2)

      #if gpc1Conn:
      #   gpc1DiffInputSkyfileRow = getGPC1diffInputSkyfile(gpc1Conn, result["imageid"])
      #else:
      #   #print "No remote database available.  Connecting to local database."
      #   #gpc1DiffInputSkyfileRow = getGPC1diffInputSkyfile(conn, result["imageid"])
      #   print "No remote database available.  Cannot connect to local DB for the time being, so skipping this row.."
      #   continue

      #gpc1DiffInputSkyfileRow = getGPC1diffInputSkyfile(conn, result["imageid"])

      #if not gpc1DiffInputSkyfileRow:
      #   print "No matching rows for diffInputSkyfile %s. Skipping to the next row." % result["imageid"]
      #   continue
 
      diffImageCombination = findIdCombinationForPostageStampRequest2(result)

      if not diffImageCombination:
         print("No valid image combination for diffInputSkyfile" % result["imageid"])
         continue

      # 2012-09-21 KWS Discovered that PyFITS3 doesn't allow implicit creation of
      #                double arrays from integer lists.  Need to cast integers
      #                as floats.
      # 2013-11-28 KWS For 3pi the result["ra_psf"] and result["dec_psf"] are in SQL Decimal format.
      #                They need to be cast as floats.
      # 2021-12-28 KWS Pull out the camera from the fpa_detector value - use it to specify gpc1 or gpc2.
      rownum.append(row)

      try:
          project.append(result["fpa_detector"].lower())
      except KeyError as e:
          project.append(camera)

      survey_name.append('null')
      ipp_release.append('null')
      job_type.append('stamp')
      option_mask.append(optionMask)  # Changed to 2049 for unconvolved stacks
      req_type.append(psRequestType)
      img_type.append(diffImageCombination[imageType][0])  # Finder type - usually "stack" for MD images
      id.append(diffImageCombination[imageType][1]) # This should contain the target ID as extracted from the GPC1 database
      tess_id.append(result["field"])
      component.append('skycell.' + str(result["sc"])) # Changed to extract skycell info directly from CMF header
      coord_mask.append(2)
      center_x.append(float(result["ra_psf"]))
      center_y.append(float(result["dec_psf"]))
      width.append(float(xsize))
      height.append(float(ysize))
      #label.append('null')
      data_group.append('null')
      reqfilt.append('null')
      mjd_min.append(0)
      mjd_max.append(0)
      run_type.append('null')
      fwhm_min.append(0)
      fwhm_max.append(0)
      # Added IPP_IDET to list of columns selected
      comment.append('%d_%s_%s_%d_%s' % (result["id"], result["tdate"], result["imageid"], result["ipp_idet"], imageType)) # Hard wired "finder" as image type
      row = row + 1

   # Create the FITS columns.

   rownum_col = pf.Column(name='ROWNUM', format='J', array=rownum)
   project_col = pf.Column(name='PROJECT', format='16A', array=project)
   survey_name_col = pf.Column(name='SURVEY_NAME', format='16A', array=survey_name)
   ipp_release_col = pf.Column(name='IPP_RELEASE', format='16A', array=ipp_release)
   job_type_col = pf.Column(name='JOB_TYPE', format='16A', array=job_type)

   option_mask_col = pf.Column(name='OPTION_MASK', format='J', array=option_mask)

   req_type_col = pf.Column(name='REQ_TYPE', format='16A', array=req_type)
   img_type_col = pf.Column(name='IMG_TYPE', format='16A', array=img_type)
   id_col = pf.Column(name='ID', format='16A', array=id)
   tess_id_col = pf.Column(name='TESS_ID', format='64A', array=tess_id)
   component_col = pf.Column(name='COMPONENT', format='64A', array=component)

   coord_mask_col = pf.Column(name='COORD_MASK', format='J', array=coord_mask)
   center_x_col = pf.Column(name='CENTER_X', format='D', array=center_x)
   center_y_col = pf.Column(name='CENTER_Y', format='D', array=center_y)
   width_col = pf.Column(name='WIDTH', format='D', array=width)
   height_col = pf.Column(name='HEIGHT', format='D', array=height)

   #label_col = pf.Column(name='LABEL', format='64A', array=label)
   data_group_col = pf.Column(name='DATA_GROUP', format='64A', array=data_group)
   reqfilt_col = pf.Column(name='REQFILT', format='16A', array=reqfilt)
   mjd_min_col = pf.Column(name='MJD_MIN', format='D', array=mjd_min)
   mjd_max_col = pf.Column(name='MJD_MAX', format='D', array=mjd_max)
   run_type_col = pf.Column(name='RUN_TYPE', format='16A', array=run_type)
   fwhm_min_col = pf.Column(name='FWHM_MIN', format='D', array=fwhm_min)
   fwhm_max_col = pf.Column(name='FWHM_MAX', format='D', array=fwhm_max)
   comment_col = pf.Column(name='COMMENT', format='64A', array=comment)

   cols=pf.ColDefs([rownum_col,
                    project_col,
                    survey_name_col,
                    ipp_release_col,
                    job_type_col,
                    option_mask_col,
                    req_type_col,
                    img_type_col,
                    id_col,
                    tess_id_col,
                    component_col,
                    coord_mask_col,
                    center_x_col,
                    center_y_col,
                    width_col,
                    height_col,
                    data_group_col,
                    reqfilt_col,
                    mjd_min_col,
                    mjd_max_col,
                    run_type_col,
                    fwhm_min_col,
                    fwhm_max_col,
                    comment_col])

   tbhdu=pf.BinTableHDU.from_columns(cols)

   hdulist.append(tbhdu)
   exthdr = hdulist[1].header

   exthdr.set('EXTNAME','PS1_PS_REQUEST','name of this binary table extension')
   exthdr.set('REQ_NAME',requestName,'Postage Stamp request name')

   # 2015-03-24 KWS Updated contents of the header for version 2
   exthdr.set('EXTVER','2','Extension version')
   exthdr.set('ACTION','PROCESS')
   exthdr.set('EMAIL',email,'Email address of submitter')
   hdulist.writeto(outfile, overwrite=True)

   fileSuccessfullyWritten = True

   return fileSuccessfullyWritten



# 2014-03-05 KWS New function to request a finder by its coordinates only. This can be used
#                by NON PS1 code to request finders.  It is not dependent on the PS1 database.
# 2015-03024 KWS Updated the FITS table to V2 format.

def writeFinderPostageStampRequestByCoords(outfile, requestName, objectList, sizeInArcsec, filter, optionMask = 2049, imageType = 'stack', allSkycells = False, surveyName = 'null', ippRelease = 'null', email = 'qub2@qub.ac.uk', camera = 'gpc1', mjdMin = 0, mjdMax = 0):
   """writeFinderPostageStampRequestByCoords.

   Args:
       outfile:
       requestName:
       objectList:
       sizeInArcsec:
       filter:
       optionMask:
       imageType:
       allSkycells:
       surveyName:
       ippRelease:
       email:
       camera:
       mjdMin:
       mjdMax:
   """

   # "objectList" is the data set returned from the database of all the candidates.  Need
   # to construct a suitable query that contains the appropriate columns.
   fileSuccessfullyWritten = False

   hdu = pf.PrimaryHDU()
   hdulist = pf.HDUList()
   prihdr = hdu.header

   prihdr.set('SIMPLE', True, 'file does conform to FITS standard')
   prihdr.set('BITPIX', 16, comment='number of bits per data pixel')
   prihdr.set('NAXIS', 0, comment='number of data axes')
   prihdr.set('EXTEND', True, 'FITS dataset may contain extensions')
   prihdr.add_comment("  FITS (Flexible Image Transport System) format is defined in 'Astronomy")
   prihdr.add_comment("  and Astrophysics', volume 376, page 359; bibcode: 2001A&A...376..359H")
   hdulist.append(hdu)

   rownum = []
   project = []
   survey_name = []
   ipp_release = []
   job_type = []
   option_mask = []
   req_type = []
   img_type = []
   id = []
   tess_id = []
   component = []
   coord_mask = []
   center_x = [] # RA
   center_y = [] # DEC
   width = []
   height = []
   #label = []
   data_group = []
   reqfilt = []
   mjd_min = []
   mjd_max = []
   run_type = []
   fwhm_min = []
   fwhm_max = []
   comment = []


   row = 1

   # No need for a remote connection for coord requests.

   for result in objectList:
      if 'V3' in result["field"]:
         xsize = int(sizeInArcsec/0.25)
         ysize = int(sizeInArcsec/0.25)
      else:
         xsize = int(sizeInArcsec/0.2)
         ysize = int(sizeInArcsec/0.2)

      rownum.append(row)
      project.append(camera)
      # 2014-09-16 KWS Added Survey Name and IPP Release
      survey_name.append(surveyName)
      ipp_release.append(ippRelease)
      job_type.append('stamp')
      option_mask.append(optionMask)
      req_type.append('bycoord')
      img_type.append(imageType)  # Finder type - usually "stack" for MD images
      id.append(0)
      tess_id.append(result["field"])
      component.append('null')
      coord_mask.append(2)
      center_x.append(float(result["ra_psf"]))
      center_y.append(float(result["dec_psf"]))
      if allSkycells:
         width.append(float(100))
         height.append(float(100))
      else:
         width.append(float(xsize))
         height.append(float(ysize))
      #label.append('null')
      data_group.append('null')
      reqfilt.append(filter)
      mjd_min.append(mjdMin)
      mjd_max.append(mjdMax)
      run_type.append('deep')
      fwhm_min.append(0)
      fwhm_max.append(0)
      comment.append('%s' % (result["id"]))

      row = row + 1

   # Create the FITS columns.

   rownum_col = pf.Column(name='ROWNUM', format='J', array=rownum)
   project_col = pf.Column(name='PROJECT', format='16A', array=project)
   survey_name_col = pf.Column(name='SURVEY_NAME', format='16A', array=survey_name)
   ipp_release_col = pf.Column(name='IPP_RELEASE', format='16A', array=ipp_release)
   job_type_col = pf.Column(name='JOB_TYPE', format='16A', array=job_type)

   option_mask_col = pf.Column(name='OPTION_MASK', format='J', array=option_mask)

   req_type_col = pf.Column(name='REQ_TYPE', format='16A', array=req_type)
   img_type_col = pf.Column(name='IMG_TYPE', format='16A', array=img_type)
   id_col = pf.Column(name='ID', format='16A', array=id)
   tess_id_col = pf.Column(name='TESS_ID', format='64A', array=tess_id)
   component_col = pf.Column(name='COMPONENT', format='64A', array=component)

   coord_mask_col = pf.Column(name='COORD_MASK', format='J', array=coord_mask)
   center_x_col = pf.Column(name='CENTER_X', format='D', array=center_x)
   center_y_col = pf.Column(name='CENTER_Y', format='D', array=center_y)
   width_col = pf.Column(name='WIDTH', format='D', array=width)
   height_col = pf.Column(name='HEIGHT', format='D', array=height)

   #label_col = pf.Column(name='LABEL', format='64A', array=label)
   data_group_col = pf.Column(name='DATA_GROUP', format='64A', array=data_group)
   reqfilt_col = pf.Column(name='REQFILT', format='16A', array=reqfilt)
   mjd_min_col = pf.Column(name='MJD_MIN', format='D', array=mjd_min)
   mjd_max_col = pf.Column(name='MJD_MAX', format='D', array=mjd_max)
   run_type_col = pf.Column(name='RUN_TYPE', format='16A', array=run_type)
   fwhm_min_col = pf.Column(name='FWHM_MIN', format='D', array=fwhm_min)
   fwhm_max_col = pf.Column(name='FWHM_MAX', format='D', array=fwhm_max)
   comment_col = pf.Column(name='COMMENT', format='64A', array=comment)

   cols=pf.ColDefs([rownum_col,
                    project_col,
                    survey_name_col,
                    ipp_release_col,
                    job_type_col,
                    option_mask_col,
                    req_type_col,
                    img_type_col,
                    id_col,
                    tess_id_col,
                    component_col,
                    coord_mask_col,
                    center_x_col,
                    center_y_col,
                    width_col,
                    height_col,
                    data_group_col,
                    reqfilt_col,
                    mjd_min_col,
                    mjd_max_col,
                    run_type_col,
                    fwhm_min_col,
                    fwhm_max_col,
                    comment_col])

   tbhdu=pf.BinTableHDU.from_columns(cols)

   hdulist.append(tbhdu)
   exthdr = hdulist[1].header

   exthdr.set('EXTNAME','PS1_PS_REQUEST','name of this binary table extension')
   exthdr.set('REQ_NAME',requestName,'Postage Stamp request name')

   # 2015-03-24 KWS Updated contents of the header for version 2
   exthdr.set('EXTVER','2','Extension version')
   exthdr.set('ACTION','PROCESS')
   exthdr.set('EMAIL',email,'Email address of submitter')
   hdulist.writeto(outfile, overwrite=True)

   fileSuccessfullyWritten = True

   return fileSuccessfullyWritten


# 2016-02-19 KWS New code to request full skycells by skycell.
def writePostageStampRequestBySkycell(outfile, requestName, skycellList, filter, optionMask = 2049, imageType = 'stack', surveyName = '3PI', ippRelease = '3PI.PV3', email = 'qub2@qub.ac.uk', camera = 'gpc1'):
   """writePostageStampRequestBySkycell.

   Args:
       outfile:
       requestName:
       skycellList:
       filter:
       optionMask:
       imageType:
       surveyName:
       ippRelease:
       email:
       camera:
   """

   # "objectList" is the data set returned from the database of all the candidates.  Need
   # to construct a suitable query that contains the appropriate columns.
   fileSuccessfullyWritten = False

   hdu = pf.PrimaryHDU()
   hdulist = pf.HDUList()
   prihdr = hdu.header

   prihdr.set('SIMPLE', True, 'file does conform to FITS standard')
   prihdr.set('BITPIX', 16, comment='number of bits per data pixel')
   prihdr.set('NAXIS', 0, comment='number of data axes')
   prihdr.set('EXTEND', True, 'FITS dataset may contain extensions')
   prihdr.add_comment("  FITS (Flexible Image Transport System) format is defined in 'Astronomy")
   prihdr.add_comment("  and Astrophysics', volume 376, page 359; bibcode: 2001A&A...376..359H")
   hdulist.append(hdu)

   rownum = []
   project = []
   survey_name = []
   ipp_release = []
   job_type = []
   option_mask = []
   req_type = []
   img_type = []
   id = []
   tess_id = []
   component = []
   coord_mask = []
   center_x = [] # RA
   center_y = [] # DEC
   width = []
   height = []
   #label = []
   data_group = []
   reqfilt = []
   mjd_min = []
   mjd_max = []
   run_type = []
   fwhm_min = []
   fwhm_max = []
   comment = []


   row = 1

   # No need for a remote connection for coord requests.

   for skycellinfo in skycellList:
      rownum.append(row)
      project.append(camera)
      survey_name.append(surveyName)
      ipp_release.append(ippRelease)
      job_type.append('stamp')
      option_mask.append(optionMask)
      req_type.append('byskycell')
      img_type.append(imageType)  # Finder type - usually "stack" for MD images
      id.append(0)
      tess_id.append('RINGS.V3')
      component.append(skycellinfo['skycell'])
      coord_mask.append(3)
      center_x.append(0.0)
      center_y.append(0.0)
      width.append(0.0)
      height.append(0.0)
      data_group.append('null')
      reqfilt.append(filter)
      mjd_min.append(0)
      mjd_max.append(0)
      run_type.append('deep')
      fwhm_min.append(0)
      fwhm_max.append(0)
      comment.append('%s' % (skycellinfo['object']))

      row = row + 1

   # Create the FITS columns.

   rownum_col = pf.Column(name='ROWNUM', format='J', array=rownum)
   project_col = pf.Column(name='PROJECT', format='16A', array=project)
   survey_name_col = pf.Column(name='SURVEY_NAME', format='16A', array=survey_name)
   ipp_release_col = pf.Column(name='IPP_RELEASE', format='16A', array=ipp_release)
   job_type_col = pf.Column(name='JOB_TYPE', format='16A', array=job_type)

   option_mask_col = pf.Column(name='OPTION_MASK', format='J', array=option_mask)

   req_type_col = pf.Column(name='REQ_TYPE', format='16A', array=req_type)
   img_type_col = pf.Column(name='IMG_TYPE', format='16A', array=img_type)
   id_col = pf.Column(name='ID', format='16A', array=id)
   tess_id_col = pf.Column(name='TESS_ID', format='64A', array=tess_id)
   component_col = pf.Column(name='COMPONENT', format='64A', array=component)

   coord_mask_col = pf.Column(name='COORD_MASK', format='J', array=coord_mask)
   center_x_col = pf.Column(name='CENTER_X', format='D', array=center_x)
   center_y_col = pf.Column(name='CENTER_Y', format='D', array=center_y)
   width_col = pf.Column(name='WIDTH', format='D', array=width)
   height_col = pf.Column(name='HEIGHT', format='D', array=height)

   #label_col = pf.Column(name='LABEL', format='64A', array=label)
   data_group_col = pf.Column(name='DATA_GROUP', format='64A', array=data_group)
   reqfilt_col = pf.Column(name='REQFILT', format='16A', array=reqfilt)
   mjd_min_col = pf.Column(name='MJD_MIN', format='D', array=mjd_min)
   mjd_max_col = pf.Column(name='MJD_MAX', format='D', array=mjd_max)
   run_type_col = pf.Column(name='RUN_TYPE', format='16A', array=run_type)
   fwhm_min_col = pf.Column(name='FWHM_MIN', format='D', array=fwhm_min)
   fwhm_max_col = pf.Column(name='FWHM_MAX', format='D', array=fwhm_max)
   comment_col = pf.Column(name='COMMENT', format='64A', array=comment)

   cols=pf.ColDefs([rownum_col,
                    project_col,
                    survey_name_col,
                    ipp_release_col,
                    job_type_col,
                    option_mask_col,
                    req_type_col,
                    img_type_col,
                    id_col,
                    tess_id_col,
                    component_col,
                    coord_mask_col,
                    center_x_col,
                    center_y_col,
                    width_col,
                    height_col,
                    data_group_col,
                    reqfilt_col,
                    mjd_min_col,
                    mjd_max_col,
                    run_type_col,
                    fwhm_min_col,
                    fwhm_max_col,
                    comment_col])

   tbhdu=pf.BinTableHDU.from_columns(cols)

   hdulist.append(tbhdu)
   exthdr = hdulist[1].header

   exthdr.set('EXTNAME','PS1_PS_REQUEST','name of this binary table extension')
   exthdr.set('REQ_NAME',requestName,'Postage Stamp request name')

   # 2015-03-24 KWS Updated contents of the header for version 2
   exthdr.set('EXTVER','2','Extension version')
   exthdr.set('ACTION','PROCESS')
   exthdr.set('EMAIL',email,'Email address of submitter')
   hdulist.writeto(outfile, overwrite=True)

   fileSuccessfullyWritten = True

   return fileSuccessfullyWritten


# 2015-03-19 KWS Need to add a tag to the object to state that we have requested images.
def addRequestIdToTransients(conn, psReqeustId, transients, processingFlag = PROCESSING_FLAGS['stamps']):
   """addRequestIdToTransients.

   Args:
       conn:
       psReqeustId:
       transients:
       processingFlag:
   """
   import MySQLdb

   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      # 2025-02-12 KWS I've no idea why processing_flags is NULL, but occasionally it is.
      #                Override its value with zero before doing the bitwise OR.
      for transient in transients:
         cursor.execute ("""
            update tcs_transient_objects
            set postage_stamp_request_id = %s, processing_flags = coalesce(processing_flags, 0) | %s
            where id = %s
            """, (psReqeustId, processingFlag, transient))

         cursor.execute ("""
            update tcs_transient_reobservations
            set postage_stamp_request_id = %s
            where transient_object_id = %s
            """, (psReqeustId, transient))

   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))

   return cursor.rowcount


def extractIdFromResponse(requestName, htmlPage):
   """extractIdFromResponse.

   Args:
       requestName:
       htmlPage:
   """

   psRequestIdPrefix = 'Request Name: &nbsp; '
   pssServerIdPrefix = 'Submitted Request ID:&nbsp; '
   errorText = 'Error:'

   pssServerId = -1

   if (htmlPage.find(errorText) > 0 or htmlPage.find(requestName) == -1 or htmlPage.find(psRequestIdPrefix) == -1 or htmlPage.find(pssServerIdPrefix) == -1):
      print("Cannot continue...  Something went wrong.")
   else:
      # Extract a subset of the page that contains our request ID.
      # This is done first, to make doubly sure that the PSS server ID
      # we pick up is the one related to our request.
      truncatedPage = htmlPage[htmlPage.find(psRequestIdPrefix)-len(pssServerIdPrefix)-10:htmlPage.find(psRequestIdPrefix)+len(psRequestIdPrefix)+len(requestName)]

      # Now extract the PSS server ID
      idString = truncatedPage[truncatedPage.find(pssServerIdPrefix)+len(pssServerIdPrefix):truncatedPage.find(psRequestIdPrefix)]
      try:
         pssServerId = int(idString)
      except ValueError:
         print("Error: Cannot convert string: '%s' to an integer." % idString)

   return pssServerId


def pretty_print_POST(req):
    """
    At this point it is completely built and ready
    to be fired; it is "prepared".

    However pay attention at the formatting used in 
    this function because it is programmed to be pretty 
    printed and may differ from the actual request.
    """
    print('{}\n{}\r\n{}\r\n\r\n{}'.format(
        '-----------START-----------',
        req.method + ' ' + req.url,
        '\r\n'.join('{}: {}'.format(k, v) for k, v in req.headers.items()),
        req.body,
    ))


# 2023-02-20 KWS Replacement code for sendPSRequest required for python 3.
#                Looks straightforward enough - but I think "realm" may need
#                to be added as a header.
def sendPSRequest(filename, requestName, username = None, password = None, postageStampServerURL = None):
    """sendPSRequest.

    Args:
        filename:
        requestName:
        username:
        password:
        postageStampServerURL:
    """
    pssServerId = -1

    import requests
    from requests import Session

    if username is None or password is None:
        print("Error: Username and Password must be present in the rquest")
        return
    try:
        data = {'filename': open(filename, 'rb')}
    except:
        print("Error: could not open file %s for reading" % filename)
        print("Check permissions on the file or folder it resides in")
        return

    s = Session()
    s.auth=(username, password)

    headers =  {'User-agent' : 'Mozilla/5.0 (X11; U; Linux x86_64; en-US; rv:1.9.0.14) Gecko/2009091106 CentOS/3.0.14-1.el5.centos Firefox/3.0.14'}

    # Need to authenticate (basic auth) so this should result in two requests to the server
    try:
        # First get our authenticated session established. Response code will be a 401
        # and a "please enter your username and password" message, but that's OK.
        # Our session is now authenticated.
        response = s.post(postageStampServerURL)
        # Now post the data. This time we should get a 200 OK.
        response = s.post(postageStampServerURL, files = data, timeout = 10, headers = headers)
        pssServerId = extractIdFromResponse(requestName, response.text)

    except IOError as e:
        print("Something went horribly wrong. File was not uploaded.")
        print(e)

    if (pssServerId >= 0):
        print("Successfully uploaded file.")
        print("Postage Stamp Server ID = %d" % pssServerId)

    return pssServerId


# 2014-03-05 Added and modified code from Thomas Chen that facilitates PSPS requesting of postage stamps

PSPS_HOST_URL = "http://web01.psps.ifa.hawaii.edu"
PSPS_STAMP_URL = "/PSI/postage_stamp.php"
PSPS_FORWARD_URL = "/PSI/index.php"

class PSPSLoginAndStampRequest(object):
    """
    # Script to log in to website and store cookies. 
    # run as: python web_login.py USERNAME PASSWORD
    #
    # http://martinjc.com/2011/06/09/logging-in-to-websites-with-python/
    #
    # sources of code include:
    # 
    # http://stackoverflow.com/questions/2954381/python-form-post-using-urllib2-also-question-on-saving-using-cookies
    # http://stackoverflow.com/questions/301924/python-urllib-urllib2-httplib-confusion
    # http://www.voidspace.org.uk/python/articles/cookielib.shtml
    #
    # mashed together by Martin Chorley
    # 
    # Licensed under a Creative Commons Attribution ShareAlike 3.0 Unported License.
    # http://creativecommons.org/licenses/by-sa/3.0/
    """

    def __init__(self, username, password, ra, dec, filter, size):
        """__init__.

        Args:
            username:
            password:
            ra:
            dec:
            filter:
            size:
        """
        
        # url for website we want to log in to
        self.base_url = 'http://web01.psps.ifa.hawaii.edu'
        # login action we want to post data to
        # could be /login or /account/login or something similar
        self.login_action = '/PSI/login.php'
        # file for storing cookies
        self.cookie_file = 'login.cookies'

        # user provided username and password
        self.username = username
        self.password = password

        # set up a cookie jar to store cookies
        self.cj = http.cookiejar.MozillaCookieJar(self.cookie_file)

        # set up opener to handle cookies, redirects etc
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPRedirectHandler(),
            urllib.request.HTTPHandler(debuglevel=0),
            urllib.request.HTTPSHandler(debuglevel=0),
            urllib.request.HTTPCookieProcessor(self.cj)
        )

        # pretend we're a web browser and not a python script
        self.opener.addheaders = [('User-agent', 
            ('Mozilla/5.0 (X11; U; Linux x86_64; en-US; rv:1.9.0.14) Gecko/2009091106 '
            'CentOS/3.0.14-1.el5.centos Firefox/3.0.14'))
        ]

        # open the front page of the website to set and save initial cookies
        response = self.opener.open(self.base_url)
        self.cj.save()

        # try and log in to the site
        response = self.login()

        #print response.read()
        #print "OK1"

        data = urllib.parse.urlencode({
            'selectSurveyID' : '3PI',
            'selectReleaseName' : ' ',
            'selectImageType' : "stack",
            'selectImageSource' : "coord",
            'radioCoordRangeType' : "pixel",
            'textCoordPixelWidth' : str(size),
            'textCoordPixelHeight' : str(size),
            'radioCoordSrc' : "form",
            'textCoordCenterRa_1' : str(ra),
            'textCoordCenterDec_1' : str(dec),
            'selectFilter_1' : str(filter),
            'option_SELECT_IMAGE' : "Y",
            'option_SELECT_CMF' : "Y",
            'btnShowMoreOptionsVal' : "1", # Changed to 1 because we want CMF files
            'IsPostBack' : "Y",
            'btnDoSubmit' : "run",
            # additional params added by kws
            #'textCoordRangeRa' : "",
            #'textCoordRangeDec' : "",
            #'textExposureID' : "",
            #'textImageSource' : "",
            #'textSkyCell' : "",
            #'textTessellationID' : "",
            #'textSkyCellOptional' : "",
            #'textOta' : "",
            #'textDiffDet' : "",
            #'selectMyDBTable' : "",
            'selectMyDBRows' : "1",
            'useMjdDate' : "0",
            'selectCoordUnit_1' : "deg",
            #'textMjdMin_1' : "",
            #'textMjdMax_1' : "",
            'CoordCount' : "1",
        })

        print(data)
        #response = self.opener.open(PSPS_HOST_URL + PSPS_STAMP_URL, data)
        #print response.read()

        print("OK")

        
    # method to do login
    def login(self):
        """login.
        """

        # parameters for login action
        # may be different for different websites
        # check html source of website for specifics
        login_data = urllib.parse.urlencode({
            'userid' : self.username,
            'password' : self.password,
            'forwardURL' : PSPS_FORWARD_URL,
            'submitLogin' : "Login"
        })

        # construct the url
        login_url = self.base_url + self.login_action
        # then open it
        response = self.opener.open(login_url, login_data)
        # save the cookies and return the response
        self.cj.save()
        return response

# Extract the filter from the PHOTCODE

photcodeMap = {'7103':'V',
               '7200':'w',
               '7213':'g',
               '7214':'r',
               '7215':'i'}

def createFakeCMFFromPhotpipeCat(filename, outputFilename = None, testData = False):
   """createFakeCMFFromPhotpipeCat.

   Args:
       filename:
       outputFilename:
       testData:
   """
   #from astropy.wcs import wcs # We may need to modify which wcs library we use. This one is part of Ureka.
   #import pywcs as wcs 
   import csv, io
   import numpy as n
   from math import log10
   from gkutils.commonutils import xy2sky

   # 2014-08-05 KWS Added Signal/Noise cut when creating the CMF file.
   SNR = 5.0

   if not outputFilename:
      outputFilename = filename + '.cmf'

   directory = os.path.dirname(filename)

   header, dataDictFile = readPhotpipeDCMPFile(filename)

#   h = pf.open(filename)
#   h.verify('fix') # Non-standard keywords.  Tell PyFITS to fix them.  Can't use WCS without this.

   # These photpipe dcmp files are ASCII files with a FITS header.  PyFITS *will* complain
   # but we should be able to get hold of the header OK.

#   header = h[0].header
   del header['COMMENT']

   # We want to write this header and a new binary FITS table which will constitute the fake
   # CMF file that we will ingest.  We then want to read the ascii data and write them to
   # the binary table.

#   data = h[0].data.tostring()

#   headerFields = []

#   try:
#      for i in range(30):
#         headerFields.append(header['COLTBL%s'%(i+1)])
#   except KeyError as e:
#      print "Cannot read the DCMP table column headers."
#      print e
#      return 1
#
#   headerLine = ' '.join(headerFields)
#   headerLine += '\n'
#
#   asciiData = headerLine + data
#
#   dataDictFile = csv.DictReader(StringIO.StringIO(asciiData), delimiter=' ', skipinitialspace = True)
#
#   # Extract the MJD from the source images (somehow!!)




   try:
      filter = photcodeMap[header['PHOTCODE'][-4:]] # last 4 characters
      mjd = header['MJD-OBS']
      # The input images for the diff can be found here:
      diffCmd = header['DIFFCMD'].split(' ')
      pointingName = header['OBJECT']

      # We may need to come back to the choice of zero pt
      zeroPt = header['ZPTMAGAV']
      # The input, output and template images can be extracted from the diff command, but
      # they are also stored in relevant FITS cards.

      # inputImage = diffCmd[2]
      # templateImage = diffCmd[4]
      # outputImage = diffCmd[6]

      inputImage = header['TARGET']
      templateImage = header['TEMPLATE']
      outputImage = header['DIFFIM']         

      noiseImage = diffCmd[8]
      maskImage = diffCmd[10]
   except KeyError as e:
      print("Vital header is missing from the file.  Cannot continue.")
      print(e)
      return 1


   # Example DIFFCMD:

   # 0 hotpants
   # 1 -inim
   # 2 /data/psdb2data2/atlas/data/v10.0/PATHTAK/DEFAULT/workspace/140125/1/opp173_127_1p16_1.V.140125.1.stk_1.sw.fits
   # 3 -tmplim
   # 4 /data/psdb2data2/atlas/data/v10.0/PATHTAK/DEFAULT/workspace/140124/1/opp173_127_1p16_1.V.140124.4.stk_1.sw.fits
   # 5 -outim
   # 6 /data/psdb2data2/atlas/data/v10.0/PATHTAK/DEFAULT/workspace/140125_140124/1/opp173_127_1p16_1.V.140125.1.stk_1_opp173_127_1p16_1.V.140124.4.stk_1.diff.fits
   # 7 -ini
   # 8 /data/psdb2data2/atlas/data/v10.0/PATHTAK/DEFAULT/workspace/140125/1/opp173_127_1p16_1.V.140125.1.stk_1.sw.noise.fits
   # 9 -imi
   # 10 /data/psdb2data2/atlas/data/v10.0/PATHTAK/DEFAULT/workspace/140125/1/opp173_127_1p16_1.V.140125.1.stk_1.sw.mask.fits
   # 11 -il
   # 12 0
   # 13 -iu
   # 14 42860
   # 15 -iuk
   # 16 25716
   # 17 -tni
   # 18 /data/psdb2data2/atlas/data/v10.0/PATHTAK/DEFAULT/workspace/140124/1/opp173_127_1p16_1.V.140124.4.stk_1.sw.noise.fits
   # 19 -tmi
   # 20 /data/psdb2data2/atlas/data/v10.0/PATHTAK/DEFAULT/workspace/140124/1/opp173_127_1p16_1.V.140124.4.stk_1.sw.mask.fits
   # 21 -tl
   # 22 0
   # 23 -tu
   # 24 42864
   # 25 -tuk
   # 26 25718
   # 27 -nrx
   # 28 1
   # 29 -nry
   # 30 1
   # 31 -nsx
   # 32 40
   # 33 -nsy
   # 34 60
   # 35 -nss
   # 36 7
   # 37 -ng
   # 38 3
   # 39 6
   # 40 0.500
   # 41 4
   # 42 1.000
   # 43 2
   # 44 2.000
   # 45 -rss
   # 46 10
   # 47 -ft
   # 48 5.0
   # 49 -r
   # 50 4
   # 51 -ko
   # 52 2
   # 53 -bgo
   # 54 1
   # 55 -ssig
   # 56 3.0
   # 57 -ks
   # 58 2.0
   # 59 -kfm
   # 60 0.99
   # 61 -okn
   # 62 -n
   # 63 i
   # 64 -sconv
   # 65 -cmp
   # 66 /data/psdb2data2/atlas/data/v10.0/PATHTAK/DEFAULT/workspace/140125_140124/1/opp173_127_1p16_1.V.140125.1.stk_1_opp173_127_1p16_1.V.140124.4.stk_1.diff.substamps
   # 67 -afssc
   # 68 0
   # 69 -sht
   # 70 -obs
   # 71 1.0
   # 72 -obz
   # 73 0.0
   # 74 -fi
   # 75 0
   # 76 -oni
   # 77 /data/psdb2data2/atlas/data/v10.0/PATHTAK/DEFAULT/workspace/140125_140124/1/opp173_127_1p16_1.V.140125.1.stk_1_opp173_127_1p16_1.V.140124.4.stk_1.diff.noise.fits
   # 78 -nsht
   # 79 -nbs
   # 80 0.1
   # 81 -nbz
   # 82 3276.80
   # 83 -fin
   # 84 0
   # 85 -mins
   # 86 2.0
   # 87 -omi
   # 88 /data/psdb2data2/atlas/data/v10.0/PATHTAK/DEFAULT/workspace/140125_140124/1/opp173_127_1p16_1.V.140125.1.stk_1_opp173_127_1p16_1.V.140124.4.stk_1.diff.mask.fits
   # 89 -mous
   # 90 0.0
   # 91 -oki
   # 92 /data/psdb2data2/atlas/data/v10.0/PATHTAK/DEFAULT/workspace/140125_140124/1/opp173_127_1p16_1.V.140125.1.stk_1_opp173_127_1p16_1.V.140124.4.stk_1.diff.kernel



   # The following is an example row out of a Photpipe diff dcmp image.

   # 'pixchk_Nneg': '11'
   # 'dflux': '319.03'
   # 'sigy': '0.629336'
   # 'sigx': '0.643638'
   # 'FWHM': '1.50'
   # 'Xpos': '514.61'
   # 'angle': '14.1'
   # 'sky': '8.00'
   # 'Ypos': '495.16'
   # 'pixchk_Nmask': '2'
   # 'pixchk_Npos': '0'
   # 'RA': '0.00000000'
   # 'chisqr': '47.42'
   # 'type': '0x00000081'
   # 'extendedness': '-6.7'
   # 'pixchk_Fneg': '-56501.00'
   # 'M': '-12.041'
   # 'flag': '0x0001'
   # 'Nmask': '2'
   # 'Dec': '0.00000000'
   # 'class': '0.00'
   # 'dM': '0.005'
   # 'pixchk_Ntot': '16'
   # 'pixchk_Fpos': '0.00'
   # 'sigxy': '-0.029656'
   # 'mask': '0x8085'
   # 'flux': '-65508.46'
   # 'peakflux': '22122.50'
   # 'FWHM1': '1.52'
   # 'FWHM2': '1.48'

   dcmpToCMF = {'Xpos': 'X_PSF',
                'Ypos': 'Y_PSF',
                'M': 'PSF_INST_MAG',
                'dM': 'PSF_INST_MAG_SIG',
                'flux': 'PSF_INST_FLUX',
                'dflux': 'PSF_INST_FLUX_SIG',
                'type': 'PADDING',
                'peakflux': 'PEAK_FLUX_AS_MAG',
                'sigx': 'MOMENTS_XX',
                'sigxy': 'MOMENTS_XY',
                'sigy': 'MOMENTS_YY',
                'sky': 'SKY',
                'chisqr': 'PSF_CHISQ',
                'class': 'DIFF_R_P',
                'FWHM1': 'PSF_MAJOR',
                'FWHM2': 'PSF_MINOR',
                'FWHM': 'DIFF_SN_P',
                'angle': 'POSANGLE',
                'extendedness': 'EXT_NSIGMA',
                'flag': 'FLAGS',
                'mask': 'FLAGS2',
                'Nmask': 'N_FRAMES',
                'RA': 'RA_PSF',
                'Dec': 'DEC_PSF',
                'pixchk_Npos': 'DIFF_NPOS',
                'pixchk_Nneg': 'PSF_NDOF',
                'pixchk_Nmask': 'PSF_NPIX',
                'pixchk_Ntot': 'DIFF_R_M',
                'pixchk_Fpos': 'DIFF_FRATIO',
                'pixchk_Fneg': 'DIFF_NRATIO_ALL'}

   cmfToDcmp = dict((v,k) for k,v in dcmpToCMF.items())

   # Initialise the column lists

   ipp_idet = []
   x_psf = []
   y_psf = []
   x_psf_sig = []
   y_psf_sig = []
   posangle = []
   pltscale = []
   psf_inst_mag = []
   psf_inst_mag_sig = []
   psf_inst_flux = []
   psf_inst_flux_sig = []
   ap_mag = []
   ap_mag_raw = []
   ap_mag_radius = []
   ap_flux = []
   ap_flux_sig = []
   peak_flux_as_mag = []
   cal_psf_mag = []
   cal_psf_mag_sig = []
   ra_psf = []
   dec_psf = []
   sky = []
   sky_sigma = []
   psf_chisq = []
   cr_nsigma = []
   ext_nsigma = []
   psf_major = []
   psf_minor = []
   psf_theta = []
   psf_qf = []
   psf_qf_perfect = []
   psf_ndof = []
   psf_npix = []
   moments_xx = []
   moments_xy = []
   moments_yy = []
   moments_r1 = []
   moments_rh = []
   kron_flux = []
   kron_flux_err = []
   kron_flux_inner = []
   kron_flux_outer = []
   diff_npos = []
   diff_fratio = []
   diff_nratio_bad = []
   diff_nratio_mask = []
   diff_nratio_all = []
   diff_r_p = []
   diff_sn_p = []
   diff_r_m = []
   diff_sn_m = []
   flags = []
   flags2 = []
   n_frames = []
   padding = []

   #wcs = wcs.WCS(header)
   
   # The RA and Dec are blank in this file. Use the WCS info to fill them in.
   i = 0
   for row in dataDictFile:
      flux = float(row[cmfToDcmp['PSF_INST_FLUX']])
      flux_sig = float(row[cmfToDcmp['PSF_INST_FLUX_SIG']])
      if flux > 0 and abs(flux_sig) > 0 and abs(flux/flux_sig) > SNR:
         #ra, dec = wcs.wcs_pix2sky(float(row['Xpos']),float(row['Ypos']), 1)
#         ra, dec = xy2sky(filename, row['Xpos'], row['Ypos'])

         ra_psf.append(float(row[cmfToDcmp['RA_PSF']]))
         dec_psf.append(float(row[cmfToDcmp['DEC_PSF']]))

         ipp_idet.append(i)
         x_psf.append(float(row[cmfToDcmp['X_PSF']]))
         y_psf.append(float(row[cmfToDcmp['Y_PSF']]))
         x_psf_sig.append(n.nan)
         y_psf_sig.append(n.nan)
         posangle.append(float(row[cmfToDcmp['POSANGLE']]))
         pltscale.append(6.2) # ATLAS ucam pixel scale (arcsec)

         instMag = float(row[cmfToDcmp['PSF_INST_MAG']])
         psf_inst_mag.append(instMag)
         psf_inst_mag_sig.append(float(row[cmfToDcmp['PSF_INST_MAG_SIG']]))

         psf_inst_flux.append(flux)
         psf_inst_flux_sig.append(float(row[cmfToDcmp['PSF_INST_FLUX_SIG']]))
         ap_mag.append(float(row[cmfToDcmp['PSF_INST_MAG']]))
         ap_mag_raw.append(float(row[cmfToDcmp['PSF_INST_MAG']]))
         ap_mag_radius.append(n.nan)
         ap_flux.append(n.nan)
         ap_flux_sig.append(n.nan)
         peak_flux_as_mag.append(float(row[cmfToDcmp['PEAK_FLUX_AS_MAG']]))

         # We only want the POSTIVE diff flux objects
         if flux > 0:
             # I checked that the (instrumental) mag is definitely -2.5*log10(flux), but I need to double-check
             # that I'm applying the correct zero point.
            calMag = instMag + header['ZPTMAGAV']
         else:
            calMag = n.nan

         cal_psf_mag.append(calMag)
         cal_psf_mag_sig.append(float(row[cmfToDcmp['PSF_INST_MAG_SIG']]))
#         ra_psf.append(ra)
#         dec_psf.append(dec)
         #ra_psf.append(ra[0])
         #dec_psf.append(dec[0])
         sky.append(float(row[cmfToDcmp['SKY']]))
         sky_sigma.append(n.nan)
         psf_chisq.append(float(row[cmfToDcmp['PSF_CHISQ']]))
         cr_nsigma.append(n.nan)
         ext_nsigma.append(float(row[cmfToDcmp['EXT_NSIGMA']]))
         psf_major.append(float(row[cmfToDcmp['PSF_MAJOR']]))
         psf_minor.append(float(row[cmfToDcmp['PSF_MINOR']]))
         psf_theta.append(n.nan)
         psf_qf.append(n.nan)
         psf_qf_perfect.append(n.nan)
         psf_ndof.append(int(row[cmfToDcmp['PSF_NDOF']]))
         psf_npix.append(int(row[cmfToDcmp['PSF_NPIX']]))
         moments_xx.append(float(row[cmfToDcmp['MOMENTS_XX']]))
         moments_xy.append(float(row[cmfToDcmp['MOMENTS_XY']]))
         moments_yy.append(float(row[cmfToDcmp['MOMENTS_YY']]))
         moments_r1.append(n.nan)
         moments_rh.append(n.nan)
         kron_flux.append(n.nan)
         kron_flux_err.append(n.nan)
         kron_flux_inner.append(n.nan)
         kron_flux_outer.append(n.nan)
         diff_npos.append(int(row[cmfToDcmp['DIFF_NPOS']]))
         diff_fratio.append(float(row[cmfToDcmp['DIFF_FRATIO']]))
         diff_nratio_bad.append(n.nan)
         diff_nratio_mask.append(n.nan)
         diff_nratio_all.append(float(row[cmfToDcmp['DIFF_NRATIO_ALL']]))
         diff_r_p.append(float(row[cmfToDcmp['DIFF_R_P']]))
         diff_sn_p.append(float(row[cmfToDcmp['DIFF_SN_P']]))
         diff_r_m.append(float(row[cmfToDcmp['DIFF_R_M']]))
         diff_sn_m.append(n.nan)
         flags.append(int(row[cmfToDcmp['FLAGS']], 16))
         flags2.append(int(row[cmfToDcmp['FLAGS2']], 16))
         n_frames.append(int(row[cmfToDcmp['N_FRAMES']]))
         padding.append(int(row[cmfToDcmp['PADDING']], 16))

      i += 1

   ipp_idet_col = pf.Column(name = 'IPP_IDET', format = '1J', bscale = 1, bzero = 2147483648, array = ipp_idet)
   x_psf_col = pf.Column(name = 'X_PSF', format = '1E', array = x_psf)
   y_psf_col = pf.Column(name = 'Y_PSF', format = '1E', array = y_psf)
   x_psf_sig_col = pf.Column(name = 'X_PSF_SIG', format = '1E', array = x_psf_sig)
   y_psf_sig_col = pf.Column(name = 'Y_PSF_SIG', format = '1E', array = y_psf_sig)
   posangle_col = pf.Column(name = 'POSANGLE', format = '1E', array = posangle)
   pltscale_col = pf.Column(name = 'PLTSCALE', format = '1E', array = pltscale)
   psf_inst_mag_col = pf.Column(name = 'PSF_INST_MAG', format = '1E', array = psf_inst_mag)
   psf_inst_mag_sig_col = pf.Column(name = 'PSF_INST_MAG_SIG', format = '1E', array = psf_inst_mag_sig)
   psf_inst_flux_col = pf.Column(name = 'PSF_INST_FLUX', format = '1E', array = psf_inst_flux)
   psf_inst_flux_sig_col = pf.Column(name = 'PSF_INST_FLUX_SIG', format = '1E', array = psf_inst_flux_sig)
   ap_mag_col = pf.Column(name = 'AP_MAG', format = '1E', array = ap_mag)
   ap_mag_raw_col = pf.Column(name = 'AP_MAG_RAW', format = '1E', array = ap_mag_raw)
   ap_mag_radius_col = pf.Column(name = 'AP_MAG_RADIUS', format = '1E', array = ap_mag_radius)
   ap_flux_col = pf.Column(name = 'AP_FLUX', format = '1E', array = ap_flux)
   ap_flux_sig_col = pf.Column(name = 'AP_FLUX_SIG', format = '1E', array = ap_flux_sig)
   peak_flux_as_mag_col = pf.Column(name = 'PEAK_FLUX_AS_MAG', format = '1E', array = peak_flux_as_mag)
   cal_psf_mag_col = pf.Column(name = 'CAL_PSF_MAG', format = '1E', array = cal_psf_mag)
   cal_psf_mag_sig_col = pf.Column(name = 'CAL_PSF_MAG_SIG', format = '1E', array = cal_psf_mag_sig)
   ra_psf_col = pf.Column(name = 'RA_PSF', format = '1D', array = ra_psf)
   dec_psf_col = pf.Column(name = 'DEC_PSF', format = '1D', array = dec_psf)
   sky_col = pf.Column(name = 'SKY', format = '1E', array = sky)
   sky_sigma_col = pf.Column(name = 'SKY_SIGMA', format = '1E', array = sky_sigma)
   psf_chisq_col = pf.Column(name = 'PSF_CHISQ', format = '1E', array = psf_chisq)
   cr_nsigma_col = pf.Column(name = 'CR_NSIGMA', format = '1E', array = cr_nsigma)
   ext_nsigma_col = pf.Column(name = 'EXT_NSIGMA', format = '1E', array = ext_nsigma)
   psf_major_col = pf.Column(name = 'PSF_MAJOR', format = '1E', array = psf_major)
   psf_minor_col = pf.Column(name = 'PSF_MINOR', format = '1E', array = psf_minor)
   psf_theta_col = pf.Column(name = 'PSF_THETA', format = '1E', array = psf_theta)
   psf_qf_col = pf.Column(name = 'PSF_QF', format = '1E', array = psf_qf)
   psf_qf_perfect_col = pf.Column(name = 'PSF_QF_PERFECT', format = '1E', array = psf_qf_perfect)
   psf_ndof_col = pf.Column(name = 'PSF_NDOF', format = '1J', array = psf_ndof)
   psf_npix_col = pf.Column(name = 'PSF_NPIX', format = '1J', array = psf_npix)
   moments_xx_col = pf.Column(name = 'MOMENTS_XX', format = '1E', array = moments_xx)
   moments_xy_col = pf.Column(name = 'MOMENTS_XY', format = '1E', array = moments_xy)
   moments_yy_col = pf.Column(name = 'MOMENTS_YY', format = '1E', array = moments_yy)
   moments_r1_col = pf.Column(name = 'MOMENTS_R1', format = '1E', array = moments_r1)
   moments_rh_col = pf.Column(name = 'MOMENTS_RH', format = '1E', array = moments_rh)
   kron_flux_col = pf.Column(name = 'KRON_FLUX', format = '1E', array = kron_flux)
   kron_flux_err_col = pf.Column(name = 'KRON_FLUX_ERR', format = '1E', array = kron_flux_err)
   kron_flux_inner_col = pf.Column(name = 'KRON_FLUX_INNER', format = '1E', array = kron_flux_inner)
   kron_flux_outer_col = pf.Column(name = 'KRON_FLUX_OUTER', format = '1E', array = kron_flux_outer)
   diff_npos_col = pf.Column(name = 'DIFF_NPOS', format = '1J', array = diff_npos)
   diff_fratio_col = pf.Column(name = 'DIFF_FRATIO', format = '1E', array = diff_fratio)
   diff_nratio_bad_col = pf.Column(name = 'DIFF_NRATIO_BAD', format = '1E', array = diff_nratio_bad)
   diff_nratio_mask_col = pf.Column(name = 'DIFF_NRATIO_MASK', format = '1E', array = diff_nratio_mask)
   diff_nratio_all_col = pf.Column(name = 'DIFF_NRATIO_ALL', format = '1E', array = diff_nratio_all)
   diff_r_p_col = pf.Column(name = 'DIFF_R_P', format = '1E', array = diff_r_p)
   diff_sn_p_col = pf.Column(name = 'DIFF_SN_P', format = '1E', array = diff_sn_p)
   diff_r_m_col = pf.Column(name = 'DIFF_R_M', format = '1E', array = diff_r_m)
   diff_sn_m_col = pf.Column(name = 'DIFF_SN_M', format = '1E', array = diff_sn_m)
   flags_col = pf.Column(name = 'FLAGS', format = '1J', bscale = 1, bzero = 2147483648, array = flags)
   flags2_col = pf.Column(name = 'FLAGS2', format = '1J', bscale = 1, bzero = 2147483648, array = flags2)
   n_frames_col = pf.Column(name = 'N_FRAMES', format = '1I', bscale = 1, bzero = 32768, array = n_frames)
   padding_col = pf.Column(name = 'PADDING', format = '1I', array = padding)

   cols = pf.ColDefs([ipp_idet_col, x_psf_col, y_psf_col, x_psf_sig_col, y_psf_sig_col, posangle_col, pltscale_col, psf_inst_mag_col, psf_inst_mag_sig_col, psf_inst_flux_col,
                     psf_inst_flux_sig_col, ap_mag_col, ap_mag_raw_col, ap_mag_radius_col, ap_flux_col, ap_flux_sig_col, peak_flux_as_mag_col, cal_psf_mag_col, cal_psf_mag_sig_col,
                     ra_psf_col, dec_psf_col, sky_col, sky_sigma_col, psf_chisq_col, cr_nsigma_col, ext_nsigma_col, psf_major_col, psf_minor_col, psf_theta_col, psf_qf_col,
                     psf_qf_perfect_col, psf_ndof_col, psf_npix_col, moments_xx_col, moments_xy_col, moments_yy_col, moments_r1_col, moments_rh_col, kron_flux_col, kron_flux_err_col,
                     kron_flux_inner_col, kron_flux_outer_col, diff_npos_col, diff_fratio_col, diff_nratio_bad_col, diff_nratio_mask_col, diff_nratio_all_col, diff_r_p_col,
                     diff_sn_p_col, diff_r_m_col, diff_sn_m_col, flags_col, flags2_col, n_frames_col, padding_col])

   # Add some new headers to indicate the location of images, for postage stamp purposes.
   # These are added to the database so that we can track the images down.

   if testData:
      # Allow for test data.  If the directory of the images is not the same as that
      # of the dcmp file, reset the directory name.

      if os.path.dirname(outputImage) != directory:
         outputImage = directory + '/' + os.path.basename(outputImage)
         header.set('DIFFIM', outputImage, 'Diff Image')

      # Even though the DCMP files contain TARGET and TEMPLATE, I've renamed them to INPUTIM and TEMPLIM,

      if os.path.dirname(inputImage) != directory:
         inputImage = directory + '/' + os.path.basename(inputImage)
      if os.path.dirname(templateImage) != directory:
         templateImage = directory + '/' + os.path.basename(templateImage)

   header.set('INPUTIM', inputImage, 'Input Image')
   header.set('TEMPLIM', templateImage, 'Template Image')

   header.set('NOISEIM', noiseImage, 'Noise Image')
   header.set('MASKIM', maskImage, 'Mask Image')
   header.set('HIERARCH FPA.ZP', zeroPt, 'Average zero pt from ZPTMAGAV')

   # Create a dummy diff skyfile ID (IMAGEID) using the last 24 bits of the md5 hash of the filename.

   m=hashlib.md5()
   m.update(filename)
   filenameMD5=m.hexdigest()
   diffId = int(filenameMD5[26:], 16) 

   header.set('IMAGEID', diffId, 'Fake Image Identifier')
   header.set('HIERARCH FPA.FILTER', filter, 'Filter as extracted from PHOTCODE')
   header.set('HIERARCH FPA.OBJECT', pointingName, 'Name of telescope pointing')

   # Now write the new CMF file
   tbhdu=pf.BinTableHDU.from_columns(cols)

   prihdu = pf.PrimaryHDU(header = header)
   hdulist = pf.HDUList([prihdu, tbhdu])

   hdulist.writeto(outputFilename, overwrite=True, output_verify='fix')

   return 0



# 2013-10-11 KWS Added detections so that we can tie them to existing images
# 2014-07-08 KWS Added extra filter definitions. (Need to fix the query code.)
def getLightcurveDetectionsAtlas(conn, candidate, filters = "grizywxBV", limit = 0):
   """getLightcurveDetectionsAtlas.

   Args:
       conn:
       candidate:
       filters:
       limit:
   """
   import MySQLdb
   from psat_server_web.atlas.atlas.commonqueries import LC_DET_QUERY_ATLAS

   try:
       cursor = conn.cursor(MySQLdb.cursors.DictCursor)
       cursor.execute (LC_DET_QUERY_ATLAS, (candidate,
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


# 2015-12-02 KWS Added new version of this code.
def getLightcurveDetectionsAtlas2(conn, candidate, limit = 0, mostRecent = True):
   """getLightcurveDetectionsAtlas2.

   Args:
       conn:
       candidate:
       limit:
       mostRecent:
   """
   import MySQLdb
   from psat_server_web.atlas.atlas.commonqueries import LC_POINTS_QUERY_ATLAS_DDT, filterWhereClause, FILTERS

   try:
       cursor = conn.cursor(MySQLdb.cursors.DictCursor)
       cursor.execute (LC_POINTS_QUERY_ATLAS_DDT + filterWhereClause(FILTERS), tuple([candidate] + [f for f in FILTERS]))
       resultSet = cursor.fetchall ()

       if limit > 0 and len(resultSet) >= limit:
           if mostRecent:
               resultSet = resultSet[-limit:]
           else:
               resultSet = resultSet[:limit]

       cursor.close ()

   except MySQLdb.Error as e:
       print("Error %d: %s" % (e.args[0], e.args[1]))
       return ()

   return resultSet


def getExistingDetectionImages(conn, candidate, ippIdetBlank = IPP_IDET_NON_DETECTION_VALUE):
   """getExistingDetectionImages.

   Args:
       conn:
       candidate:
       ippIdetBlank:
   """
   import MySQLdb
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

# 2016-10-10 KWS Added getExistingNonDetectionImages
def getExistingNonDetectionImages(conn, candidate, ippIdetBlank = IPP_IDET_NON_DETECTION_VALUE):
    """getExistingNonDetectionImages.

    Args:
        conn:
        candidate:
        ippIdetBlank:
    """

    import MySQLdb
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


def eliminateExistingImages(conn, candidate, detections, detectionsWithImages):
   """
   We'd really like to avoid requesting images that we already have.
   """

   imagesToRequest = []

   for row in detections:
      imageId = row['imageid']
      ippIdet = row['ipp_idet']

      if '%d_%s_%d_%d' % (candidate, row['tdate'], imageId, ippIdet) not in detectionsWithImages:
         imagesToRequest.append(row)

   return imagesToRequest



OBJECTTYPES = { 'all': -1,
                'orphan': 1,
                'variablestar': 2,
                'nt': 4,
                'agn': 8,
                'sn': 16 }

DETECTIONTYPES = { 'all': 1, 'detections': 2, 'nondetections': 3 }

REQUESTTYPES = { 'all': 1, 'incremental': 2}


def updateTriplets(conn, objectList):
    """updateTriplets.

    Args:
        conn:
        objectList:
    """

    for object in objectList:
        recentImageRow = getRecentImageGroupRow(conn, object)
        stamps = getPostageStampsForImageGroup(conn, recentImageRow["id"])
        imageData = {}
        mjd = target = ref = diff = None
        for stamp in stamps:
            if stamp["image_type"] == "target":
                target = stamp["image_filename"]
                mjd = stamp["mjd_obs"]

            if stamp["image_type"] == "ref":
                ref = stamp["image_filename"]

            if stamp["image_type"] == "diff":
                diff = stamp["image_filename"]

        if not mjd: # The target wasn't defined
            continue

        imageData["target"] = target
        imageData["ref"] = ref
        imageData["diff"] = diff
        imageData["mjd_obs"] = mjd

        print(imageData)
        tripletId = insertNewImageTripletReference(conn, object, imageData)
        if tripletId:
            rowsUpdated = updateObjectImageTripletReference(conn, object, tripletId)


    return (0)


def makeATLASObjectPostageStamps(conn, candidateList, PSSImageRootLocation, stampSize = 50, limit = 0, detectionType = DETECTIONTYPES['detections'], requestType = REQUESTTYPES['incremental']):
   """
   Code to create postage stamps for ATLAS objects.  The IPP_IDET values is generated and the
   IMAGEID value is the last 24 bits of the md5 hash of the filename.
   """

   # This code needs to effectively repeat the job that the postage stamp requester + downloader does.
   # We know which objects for which we need stamps, so what we need to do is create a tcs_iamge_stamps
   # record and a tcs_images record, then update tcs_transient_objects and tcs_transient_reobservations
   # accordingly.

   dx = dy = stampSize

   # Check the object is within 25 pixels of the RH or LH boundary.  WCS getfits behaves very
   # badly at the boundaries, so until we have a solution independent of WCS tools, we'll reject
   # objects at the boundary.

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
         lightcurveData = getLightcurveDetectionsAtlas(conn, candidate['id'])
         print(lightcurveData)
         existingImages = getExistingDetectionImages(conn, candidate['id'])
      else:
         # Assume a limit is set, otherwise no request will get sent. Setting a limit
         # forces only detections to be requested and this will always force the most
         # most recent <limit> images to be requested.
         if limit > 0:
            lightcurveData = getLightcurveDetectionsAtlas(conn, candidate['id'], limit = limit)

      if requestType == REQUESTTYPES['incremental'] and limit == 0:
         lightcurveData = eliminateExistingImages(conn, candidate['id'], lightcurveData, existingImages)


      if lightcurveData:
         for row in lightcurveData:
            print(row)
         imageRequestData += lightcurveData

         # We need to process a triplet of images at once.

      for row in imageRequestData:
         (objectId, tdate, diffId, ippIdet) = (row['id'], row['tdate'], row['imageid'], row['ipp_idet'])
         
         imageGroupName = "%d_%s_%s_%s" % (objectId, tdate, diffId, ippIdet)

         imageFilenames = {'target': row['inputim'], 'ref': row['templim'], 'diff': row['diffim']}

         diffFilenameDirectory = os.path.dirname(imageFilenames['diff'])

         # For testing purposes, change the target and ref image filenames to be based on the diff filename.
         if not os.path.exists(imageFilenames['target']):
            imageFilenames['target'] = diffFilenameDirectory + '/' + os.path.basename(imageFilenames['diff']).replace('.fits', '.im.fits')
            imageFilenames['ref'] = diffFilenameDirectory + '/' + os.path.basename(imageFilenames['diff']).replace('.fits', '.tmpl.fits')


         for imageType, imageName in imageFilenames.items():
            # 1. Get image stamp
            # 2. Save the stamp in the right place
            # 3. Generate a jpeg from the stamp and locate in same place as stamp
            # 4. Insert a database record in tcs_postage_stamp_images and tcs_image_groups

            outputFilename = imageGroupName + '_' + imageType + '.fits'

            errorCode = PSTAMP_SUCCESS
            # If too near to the edge set the error code to something other than zero

            # ImageMJD must be extracted from the image data.

            imageMJD = None
            imageFilterId = None
            maskedPixelRatio = None
            maskedPixelRatioAtCore = None

            localImageName = imageGroupName + '_' + imageType
            pssImageName = imageFilenames[imageType].split('/')[-1]

            flip = False

            # 2013-09-23 KWS Need to propagate crosshair colours: green1 = detection, brown1 = non-detection
            if ippIdet == str(IPP_IDET_NON_DETECTION_VALUE):
               xhColor = 'brown1'
            else:
               xhColor = 'green1'

            imageDownloadLocation = PSSImageRootLocation + '/' + "%d" % int(float(tdate))

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

            # Cut out the stamp
            status = imu.getFITSPostageStamp(imageFilenames[imageType], absoluteLocalImageName, row['x_psf'], row['y_psf'], dx, dy)

            if status == PSTAMP_SUCCESS:
               # Add the object to the set of objects that were updated.
               objectsModified.add(objectId)

               hdus = pf.open(absoluteLocalImageName)
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
                  return 1


               # Convert to JPEG & add frame & crosshairs - write image
               # 2011-07-23 KWS Need to disable flipping for V3 images.
               # 2014-06-24 KWS Change quality to 100 for ATLAS images.
               (maskedPixelRatio, maskedPixelRatioAtCore) = imu.convertFitsToJpegWithCrosshairs2(absoluteLocalImageName, imu.fitsToJpegExtension(absoluteLocalImageName), flip = flip, xhColor = xhColor, quality = 100)

               # Create an image record using IMG_NAME and COMMENT
               # This call also makes the association of the the image group with the
               # object in tcs_transient_objects and tcs_transient_reobservations.
               (imageId, imageGroupId) = insertPostageStampImageRecord(conn, localImageName, pssImageName, imageMJD, errorCode, imageFilterId, maskedPixelRatio, maskedPixelRatioAtCore)
            else:
               (imageId, imageGroupId) = insertPostageStampImageRecord(conn, localImageName, None, imageMJD, status, imageFilterId, maskedPixelRatio, maskedPixelRatioAtCore)

            hdus.close()

   # 2013-07-04 KWS We'd like to update the tcs_images table with the most recent image triplet.
   if objectsModified:
      updateTriplets(conn, objectsModified)


def getOrInsertImageGroupAtlas(conn, groupName, groupType=None, ddc = False):
   """getOrInsertImageGroupAtlas.

   Args:
       conn:
       groupName:
       groupType:
       ddc:
   """
   import MySQLdb

   rowsUpdated = 0

   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      if groupType is None:
         cursor.execute ("""
            select id from tcs_image_groups
            where name = %s
         """, (groupName,))
      else:
         cursor.execute ("""
            select id from tcs_image_groups
            where name = %s
            and group_type = %s
         """, (groupName, groupType))


      if cursor.rowcount > 0:
         groupId = cursor.fetchone ()['id']
      else:
         # We need to insert a new group
         cursor.execute ("""
            insert into tcs_image_groups (name, group_type)
            values (%s, %s)
            """, (groupName, groupType))
         groupId = conn.insert_id()

         (id, mjd, diffId, ippIdet) = groupName.split('_')

         # And we need to attach the newly created group to the associated object
         # NOTE: This strategem works fine for newly created images, but doesn't work
         #       for subsequent downloads of new image data for this observation.
         #       This may need to be re-visited.
         try:
            # Work out how many decimal places we need to truncate the
            # MJD to in our SQL query.
            (wholeMJD, fractionMJD) = mjd.split('.')
            numberOfDecimalPlaces = len(fractionMJD)
         except ValueError as e:
            numberOfDecimalPlaces = 0

         # Update tcs_transient_reobservations first (since we only
         # tend to download images of objects with multiple recurrences).
         # This should reduce the number of queries necessary.

         if groupType is None:  # Don't bother updating the transient recurrence if we're dealing with a finder

            if ddc:
                cursor.execute ("""
                   update atlas_detectionsddc r, atlas_metadataddc m
                   set r.image_group_id = %s
                   where r.atlas_object_id = %s
                   and r.det_id = %s
                   and r.atlas_metadata_id = m.id
                   and cast(truncate(m.mjd, %s) as char) = %s
                   and m.obs = %s
                   """, (groupId, id, ippIdet, numberOfDecimalPlaces, mjd, diffId))
            else:
                cursor.execute ("""
                   update atlas_diff_detections r, atlas_metadata m
                   set r.image_group_id = %s
                   where r.atlas_object_id = %s
                   and r.tphot_id = %s
                   and r.atlas_metadata_id = m.id
                   and cast(truncate(m.mjd_obs, %s) as char) = %s
                   and m.expname = %s
                   """, (groupId, id, ippIdet, numberOfDecimalPlaces, mjd, diffId))

            rowsUpdated = cursor.rowcount

            # Did we update any transient object rows? If not issue a warning.
            if rowsUpdated == 0:
               print("WARNING: No transient object entries were updated.")
               print("These images are not associated with an object in this database.")

      cursor.close ()

   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))
      sys.exit (1)

   # Did we update more than one row?  If so, the number of decimal
   # places we're using in the MJD is not fine-grained enough to
   # distinguish a unique observation.  Either that, or there is an
   # observation that has exactly the same timestamp and diff ID.
   if rowsUpdated > 1:
      print("WARNING: More than one object was updated.")
      print("The truncated MJD criteria are not fine enough.")

   return groupId



def insertPostageStampImageRecordAtlas(conn, imageName, pssName, imageMJD, pssErrorCode, filterId=None, maskedPixelRatio=None, maskedPixelRatioAtCore=None, groupType=None, ddc = False):
   """insertPostageStampImageRecordAtlas.

   Args:
       conn:
       imageName:
       pssName:
       imageMJD:
       pssErrorCode:
       filterId:
       maskedPixelRatio:
       maskedPixelRatioAtCore:
       groupType:
       ddc:
   """
   import MySQLdb
   # Find the image group
   imageId = 0
   imageGroupId = 0

   (id, mjd, diffid, ippIdet, imageType) = imageName.split('_')

   # 2014-03-10 KWS Need to append a suffix to finder groups.
   #                If this is a finder, we need to append something to the image name
   #                so that the database insert is not rejected (because of uniqueness)
   if 'finder' in imageType:
      ippIdet += 'f'

   imageGroupId = getOrInsertImageGroupAtlas(conn, '%s_%s_%s_%s' % (id, mjd, diffid, ippIdet), groupType=groupType, ddc = ddc)
   print("Image Group ID of image %s is: %s" % (imageName, imageGroupId))

   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      cursor.execute ("""
         select id from tcs_postage_stamp_images
         where image_filename = %s
      """, (imageName,))

      if cursor.rowcount > 0:
         # A record already exists.  Let's delete it and replace it with the new one.
         imageId = cursor.fetchone ()['id']
         cursor.execute("""
            delete from tcs_postage_stamp_images
            where id = %s
         """, (imageId,))

      cursor.execute ("""
         insert into tcs_postage_stamp_images (image_type, image_filename, pss_filename, mjd_obs, image_group_id, pss_error_code, filter, mask_ratio, mask_ratio_at_core)
         values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
         """, (imageType, imageName, pssName, imageMJD, imageGroupId, pssErrorCode, filterId, maskedPixelRatio, maskedPixelRatioAtCore))

      imageId = conn.insert_id()
      cursor.close ()

   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))
      sys.exit (1)

   return (imageId, imageGroupId)


# 2015-12-02 KWS New version of this code for the ATLAS ddet schema
def makeATLASObjectPostageStamps2(conn, candidateList, PSSImageRootLocation, stampSize = 200, limit = 0, mostRecent = True, detectionType = DETECTIONTYPES['detections'], requestType = REQUESTTYPES['incremental']):
   """
   Code to create postage stamps for ATLAS objects.
   """

   import subprocess

   unpackedDiffLocation='/atlas/unpacked/'
   remoteLocation = 'xfer@atlas-base-adm02.ifa.hawaii.edu:/atlas/red'
   remoteDiffLocation = 'xfer@atlas-base-adm02.ifa.hawaii.edu:/atlas/diff'
   localLocation = '/atlas/red'
   localDiffLocation = '/atlas/diff'
   rsyncFile = '/tmp/rsyncFiles_' + str(os.getpid()) + '.txt'
   rsyncDiffFile = '/tmp/rsyncDiffFiles_' + str(os.getpid()) + '.txt'
   
   funpackCmd = '/atlas/v20.0/photpipe/Cfiles/bin/linux/funpack'
   rsyncCmd = '/usr/bin/rsync'
   wpwarp1Cmd = '/atlas/bin/wpwarp1'

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


   # (1) Get the unique exposures.

   print("Finding Unique Exposures...")
   exposures = []

   for candidate in candidateList:
      lightcurveData = getLightcurveDetectionsAtlas2(conn, candidate['id'], limit = limit, mostRecent = mostRecent)
      for row in lightcurveData:
          exposures.append(row['expname'])

   exposureSet = list(set(exposures))
   exposureSet.sort()

   # (1.1) Get the diff images.  We no longer download these by default.

   print("Fetching Diff Images...")

   rsf = open(rsyncDiffFile, 'w')
   for file in exposureSet:
      camera = file[0:3]
      mjd = file[3:8]

      diffImage = camera + '/' + mjd + '/' + file + '.diff.fz'
      rsf.write('%s\n' % diffImage)

   rsf.close()

   p = subprocess.Popen([rsyncCmd, '-aux', '--files-from=%s' % rsyncDiffFile, remoteDiffLocation, localDiffLocation], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
   output, errors = p.communicate()

   if output.strip():
      print(output)
   if errors.strip():
      print(errors)

   # (2) Unpack the diff data (which we already have) to a temporary location

   print("Unpacking Diff Images...")

   rsf = open(rsyncFile, 'w')
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

   rsf.close()


   # (3) Go and get the input exposures

   print("Fetching Input Images...")

   p = subprocess.Popen([rsyncCmd, '-aux', '--files-from=%s' % rsyncFile, remoteLocation, localLocation], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
   output, errors = p.communicate()

   if output.strip():
      print(output)
   if errors.strip():
      print(errors)


   # (4) Now that we have the exposures, unpack them - same as above - should extract into a function

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
         lightcurveData = getLightcurveDetectionsAtlas2(conn, candidate['id'])
         print(lightcurveData)
         existingImages = getExistingDetectionImages(conn, candidate['id'])
      else:
         # Assume a limit is set, otherwise no request will get sent. Setting a limit
         # forces only detections to be requested and this will always force the most
         # most recent <limit> images to be requested.
         if limit > 0:
            lightcurveData = getLightcurveDetectionsAtlas2(conn, candidate['id'], limit = limit, mostRecent = mostRecent)

#      if requestType == REQUESTTYPES['incremental'] and limit == 0:
#         lightcurveData = eliminateExistingImages(conn, candidate['id'], lightcurveData, existingImages)


      if lightcurveData:
         for row in lightcurveData:
            print(row)
#         if row['x'] > stampSize/2 \
#                 and row['x'] < (10560 - stampSize/2) \
#                 and row['y'] > stampSize/2 \
#                 and row['y'] < (10560 - stampSize/2):

             # Only request images if the detection is not too near the edge of the chip

         imageRequestData += lightcurveData

         # We need to process a triplet of images at once.

      for row in imageRequestData:

         (objectId, tdate, diffId, ippIdet) = (row['id'], row['tdate'], row['expname'], row['tphot_id'])

         camera = diffId[0:3]
         mjd = diffId[3:8]
         
         imageGroupName = "%d_%s_%s_%s" % (objectId, tdate, diffId, ippIdet)

         targetImage = '/atlas/unpacked/red/' + camera + '/' + mjd + '/' + diffId + '.fits'
         diffImage = '/atlas/unpacked/diff/' + camera + '/' + mjd + '/' + diffId + '.diff'

         # We need to generate the template image using wpwarp1
         refImage = diffId + '.tmpl'

         imageFilenames = {'diff': diffImage, 'ref': refImage, 'target': targetImage}

         diffFilenameDirectory = os.path.dirname(imageFilenames['diff'])

         for imageType, imageName in imageFilenames.items():
            # 1. Get image stamp
            # 2. Save the stamp in the right place
            # 3. Generate a jpeg from the stamp and locate in same place as stamp
            # 4. Insert a database record in tcs_postage_stamp_images and tcs_image_groups

            outputFilename = imageGroupName + '_' + imageType + '.fits'

            errorCode = PSTAMP_SUCCESS
            # If too near to the edge set the error code to something other than zero

            # ImageMJD must be extracted from the image data.

            imageMJD = None
            imageFilterId = None
            maskedPixelRatio = None
            maskedPixelRatioAtCore = None

            localImageName = imageGroupName + '_' + imageType
            pssImageName = imageFilenames[imageType].split('/')[-1]

            flip = False

            # 2013-09-23 KWS Need to propagate crosshair colours: green1 = detection, brown1 = non-detection
            if ippIdet == str(IPP_IDET_NON_DETECTION_VALUE):
               xhColor = 'brown1'
            else:
               xhColor = 'green1'

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
               diffImage = imageDownloadLocation + '/' + imageGroupName + '_' + 'diff.fits'
               p = subprocess.Popen([wpwarp1Cmd, '-samp', absoluteLocalImageName, diffImage], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
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
               status = imu.getFITSPostageStamp(imageFilenames[imageType], absoluteLocalImageName, (row['x'] - 0.5), (row['y'] - 0.5), dx, dy)

            if status == PSTAMP_SUCCESS:
               print("Got stamp...")
               # Add the object to the set of objects that were updated.

               objectsModified.add(objectId)

               hdus = pf.open(absoluteLocalImageName)
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
                  return 1

               hdus.close()

               # Convert to JPEG & add frame & crosshairs - write image
               # 2011-07-23 KWS Need to disable flipping for V3 images.
               # 2014-06-24 KWS Change quality to 100 for ATLAS images.
               (maskedPixelRatio, maskedPixelRatioAtCore) = imu.convertFitsToJpegWithCrosshairs2(absoluteLocalImageName, imu.fitsToJpegExtension(absoluteLocalImageName), flip = flip, xhColor = xhColor, nsigma = 2, quality = 100, magicNumber = -31415)

               # Create an image record using IMG_NAME and COMMENT
               # This call also makes the association of the the image group with the
               # object in tcs_transient_objects and tcs_transient_reobservations.
               (imageId, imageGroupId) = insertPostageStampImageRecordAtlas(conn, localImageName, pssImageName, imageMJD, errorCode, imageFilterId, maskedPixelRatio, maskedPixelRatioAtCore)
            else:
               (imageId, imageGroupId) = insertPostageStampImageRecordAtlas(conn, localImageName, None, imageMJD, status, imageFilterId, maskedPixelRatio, maskedPixelRatioAtCore)


   # 2013-07-04 KWS We'd like to update the tcs_images table with the most recent image triplet.
#   if objectsModified:
#      updateTriplets(conn, objectsModified)

def getATLASxyFromRaDec(imageFilename, ra, dec):
    """getATLASxyFromRaDec.

    Args:
        imageFilename:
        ra:
        dec:
    """
    import subprocess
    pix2skyCmd = '/atlas/bin/pix2sky'
    # If image exists....
    x = None
    y = None
    p = subprocess.Popen([pix2skyCmd, '-sky2pix', imageFilename, str(ra), str(dec)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    output, errors = p.communicate()
    if output:
        x,y = output.split()
        try:
           x = float(x)
           y = float(y)
        except ValueError as e:
           print("Unable to convert x and y to float.")

        if x < 0 or x > 10560 or y < 0 or y > 10560:
           print("x or y out of bounds (%f, %f)" % (x, y))

    return x,y
