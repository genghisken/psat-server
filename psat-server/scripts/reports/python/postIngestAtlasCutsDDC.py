#!/usr/bin/env python
"""Refactored post ingest cut code for ATLAS.

Usage:
  %s <configfile> [<candidate>...] [--datethreshold=<datethreshold>] [--update] [--recent]
  %s (-h | --help)
  %s --version

Options:
  -h --help                         Show this screen.
  --version                         Show version.
  --datethreshold=<datethreshold>   Date threshold - no hyphens [default: 20170925].
  --update                          Update the database.
  --recent                          Check for recent objects.

  Example:
    %s ../../../../../atlas/config/config4_db5_readonly.yaml 1063629090302540900 --update
"""
import sys
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt

import os, MySQLdb, shutil, re
from gkutils.commonutils import find, Struct, cleanOptions

# 2013-05-14 KWS Complete new file for processing 3pi Diffs, based originally on the MD cuts.
#                The new file was written because it began to diverge too far from the MD cuts.

from multiprocessing import Queue
from gkutils.commonutils import dbConnect, setupLogger, getFlagDefs, FLAGS, getCurrentMJD, getMJDFromSqlDate
#from getLightcurves import getDeltas, checkDeltas

import logging

# Object quality thresholds - Global data
MXY_THRESH = 3.6                    ## XY moments of objects < 3.6
PSF_INST_MAG_SIG_THRESH_MAX = 0.2   ## 5 sigma
PSF_INST_MAG_SIG_THRESH_MIN = 0.0   ## Must be a measured value

# Recurrence Threshold - very conservative for the time being.
RECURRENCE_THRESH = 3 # 6 with non-zero MJD must mean at least 3 + 3.
INTRA_DAY_THRESH = 3 # We must have at least 3 of 6 detections within 1 day.
RECURRENCE_THRESH_RELAXED = 4
INTRA_DAY_THRESH_RELAXED = 4
INTRA_DAY_THRESH_DAYS = 1 # We must have at least 3 of 6 detections within 1 day, twice.
MJD_WINDOW = 6 # We must have at least 3 of 6 detections within 1 day, twice, within 6 days of max MJD.

# Nights threshold
NIGHTS_THRESH = 0

# Window Size Threshold
WINDOW_SIZE = 10 # Increased to 10 from 7
# 2012-12-03 KWS Reduced this to 2 as an experiment.
WINDOW_DETECTIONS_THRESH = 3

# Number of unique filters required
FILTERS_THRESH = 1

# Maximum RMS scatter. For ATLAS set this to 0.33 pixel around the object. Tighter cut for intra day objects.
RMS_SCATTER_THRESH = 20.0
# Maximum RMS scatter if we have more than our required threshold of detections.
RMS_SCATTER_THRESH_RELAXED = 1.0

# Regular Expressions.  Note that the skycell regex is 3pi compatible.
import re
FIELD_REGEX = '^([a-zA-Z0-9]+)\.'
COMPILED_FIELD_REGEX = re.compile(FIELD_REGEX)
#SKYCELL_REGEX = 'skycell\.([0-9\.]+)(.SS){0,1}\.dif\.'
# 2013-04-24 KWS New skycell regular expression to cover both MD and 3pi skycells.
SKYCELL_REGEX = 'skycell\.([0-9]+(\.[0-9]+){0,1})(\.[S|W]S){0,1}\.dif\.'
COMPILED_SKYCELL_REGEX = re.compile(SKYCELL_REGEX)

PMV_THRESH = 500
# 2018-04-09 KWS Reduced the PVR/PTR threshold down to 200 from 500.
# 2018-04-09 KWS Reduced the PVR/PTR threshold down to 0 from 200.
# 2018-04-09 KWS Increased the PVR/PTR threshold back to 100.
# 2018-08-02 KWS Reduced the PVR/PTR threshold down to 10 from 100.
# 2018-11-11 KWS Temporarily increased PVR/PTR threshold back to 100.
# 2019-10-14 KWS Reduced the threshold down to 50.
PVR_THRESH = 50
PTR_THRESH = 50

# 2020-08-13 KWS Those vertical streaks should be caught by Vartest.
#                Have been advised to set the threshold to 500 for
#                the time being.
PBN_THRESH = 500
PSC_THRESH = 500

# 2018-03-16 KWS Don't flag objects that are too close to the edge of the chip
CHIP_SIZE = 10560
MIN_SIZE = 100
MAX_SIZE = CHIP_SIZE - MIN_SIZE

#CHIN_THRESH = 50.0

from math import sqrt, atan, cos, radians

PI = 4 * atan(1.0)
RADIANS = PI/180.0

import datetime
import sys
import MySQLdb
import numpy as n

#n.set_printoptions(threshold=n.inf, linewidth=1000)

#MIN_TTI_DIFF = 0.00003 # (2013-11-12 KWS changed to approx 3 seconds - we want this to be small but non-zero)
#MAX_TTI_DIFF = 0.5 # (half a day)
# 2014-07-29 KWS For ATLAS, we want to make sure that we don't accidentally ingest data from the same pointing
#                more than once. Data from different, overlapping pointings is allowed, and it might be worth
#                keeping the min value fairly low to allow for this. If we don't want overlapping pointings,
#                then we should go for at least a 0.5 day difference.

MIN_TTI_DIFF = 0.0001 # (half a day)
MAX_TTI_DIFF = 5.0 # (5 days)

AP_MAG_MINUS_PSF_MAG_MAX = 0.2
# 2013-10-16 KWS Increased TTI mag diffs from 10.0 to 20.0 mags (i.e. no restriction)
TTI_MAG_DIFF_MAX = 20.0
ON_GHOST = 1073741824
#MIN_MJD_DIFF = 0.00003 # (2013-11-12 KWS changed to approx 2.6 seconds - we want this to be small but non-zero)
# 2018-04-21 KWS Decreased this down to 0.002 day (173 seconds).
# 2018-07-03 KWS Decreased this down to 0.0001 day (26 seconds).
MIN_MJD_DIFF = 0.0003

# 2024-07-03 KWS Need to move getDeltas and checkDeltas to gkutils.
def getDeltas(mjds):
    '''Spit out the diffs between each MJD in the list'''
    deltas = [x - mjds[i - 1] for i, x in enumerate(mjds)][1:]
    return deltas

def checkDeltas(lcData, minpoints, mjddiff, mjddiffmax = 1000, mjdWindow = 6, followupFlagDate = None):
    objectPassesChecks = False
    mjds = [x[0] for x in lcData]
    # Make sure the MJDs are sorted (especially when they are combined)
    mjds.sort()
    maxMJD = mjds[-1]

    # 2018-06-27 KWS If the earliest MJDS are outside the window, remove
    #                remove them from the list.
    for m in mjds:
        if m < maxMJD - mjdWindow:
            mjds.remove(m)

    deltas = getDeltas(mjds)

    counter = 0
    counterList = []
    # 2018-06-27 KWS Create a LIST of counters. If ONE of them passes
    #                then send a pass.  E.g. object has 3 detections on
    #                day 1 but only 1 detection on day 2. In this scenario
    #                not having a list of counters will FAIL the object,
    #                especially if the data is delayed for some reason.
    for delta in deltas:
        if delta > mjddiff and delta < mjddiffmax:
            counter += 1

        # We want n gaps within the specified time period.  If not
        # we reset the counter.  This helps us find (e.g.) objects
        # with delta > mjddiff within a day.
        if delta > mjddiffmax:
            # Park the existing counter and start a new one.
            counterList.append(counter)
            counter = 0

    # Pick up the last counter that was running.
    counterList.append(counter)

    # Now go through the ENTIRE counter list. Did ANY pass? If so, send a pass!
    for counter in counterList:
        if counter >= (minpoints - 1):
            objectPassesChecks = True
            break

    return objectPassesChecks


# 2022-01-31 KWS Get the most recently processed date.
def getMostRecentProcessedDate(conn):
    mostRecent = None
    resultSet = None
    try:
        cursor = conn.cursor (MySQLdb.cursors.DictCursor)

        cursor.execute ("""
                           select max(modified) most_recent from tcs_processing_status where status = 2
        """)

        resultSet = cursor.fetchone ()
        cursor.close ()
        if resultSet['most_recent'] is not None:
            mostRecent = resultSet['most_recent'].strftime("%Y:%m:%d %H:%M:%S")


    except MySQLdb.Error as e:
        print("Error %d: %s" % (e.args[0], e.args[1]))
        sys.exit (1)

    return mostRecent


def checkIsObjectAlreadyPromoted(conn, objectList):

    newList = []
    try:
        cursor = conn.cursor (MySQLdb.cursors.DictCursor)

        for row in objectList:
            cursor.execute( """ select id from atlas_diff_objects
                                 where id = %s
                                   and followup_id is not null
                            """, (row,))
            oid = cursor.fetchone ()
            if oid is None:
                newList.append(row)

        cursor.close ()


    except MySQLdb.Error as e:
        print("Error %d: %s" % (e.args[0], e.args[1]))
        sys.exit (1)

    return newList



# 2022-01-22 KWS Remove dependency on atlas_diff_objects. Join slows query.
def getAtlasObjectsToCheck(conn, dateThreshold = '2015-12-20 00:00:00', filterObjectsAlreadyPromoted = False):
   """
   Called by the master process.  Get all ATLAS objects which haven't been checked yet.
   This is more like the PS1 Medium Deep survey than 3pi, so let's start with the MD query.
   """
   npResultSet = []

   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      cursor.execute ("""
                         select d.atlas_object_id id
                           from atlas_detectionsddc d
                          where d.date_inserted >= %s
      """, (dateThreshold,))


      resultSet = cursor.fetchall ()
      if resultSet:
         objList = [object['id'] for object in resultSet]
         objListUnique = sorted(list(set(objList)))

         if filterObjectsAlreadyPromoted:
             objListUnique = checkIsObjectAlreadyPromoted(conn, objListUnique)
 
         npResultSet = n.array(objListUnique)
         npResultSet.sort()


      cursor.close ()

   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))
      sys.exit (1)


   return npResultSet


def getHeatMap(conn, site):

    result_set = None
    try:
        cursor = conn.cursor (MySQLdb.cursors.DictCursor)

        cursor.execute( """ select region, ndet
                              from atlas_heatmaps
                             where site = %s
                          order by region
                        """, (site,))
        result_set = cursor.fetchall ()

        cursor.close ()


    except MySQLdb.Error as e:
        print("Error %d: %s" % (e.args[0], e.args[1]))
        sys.exit (1)

    return result_set

def getSiteMasks(conn, multiplier):
   # 2022-09-07 KWS Read the heatmaps for each site.
   siteMasks = {}

   for site in ['01a', '02a', '03a', '04a']:
      siteHeatmap = getHeatMap(conn, site)

      if siteHeatmap:
         resolution = int(sqrt(len(siteHeatmap)))
         if resolution not in [8, 16, 32, 64, 128, 256, 512]:
             # Create a default 128 x 128 mask of zeros
             siteMasks[site] = n.zeros((128,128), dtype=int)
         else:
             matrix = n.zeros((resolution,resolution), dtype=int)

             for cell in siteHeatmap:
                x = cell['region'] % resolution
                y = int(cell['region'] / resolution)
                matrix[y][x] = cell['ndet']

             # Create a binary data matrix
             medValue = n.median(matrix)
             mask = (matrix > (multiplier * medValue)).astype(int)
             #print mask
             #print
             siteMasks[site] = mask
             #print mask.shape

   return siteMasks


def getMJDDiffs(objectInfo, minDiff = MIN_TTI_DIFF, maxDiff = MAX_TTI_DIFF):
    """
    Go through the array and find any combination of numbers that pass the
    minimum and maximum difference criteria.  Stop iterating when there is
    a pass.
    """

    mjds = []

    for row in objectInfo:
        mjds.append(row["MJD"])

    mjdDiffPass = False

    if len(mjds) > 1:
        # Iterate through *all* the pair combinations until we find one
        # that meets the criteria.  Then stop - no need to continue.
        for start in range(len(mjds)):
            if mjdDiffPass:
                # If the inner loop has found a pair that meets our
                # criteria, stop iterating.
                break
            for mjd1 in mjds[start+1:]:
                diff = abs(mjd1 - mjds[start])
                #print "%.5f" % diff

                if diff > minDiff and diff < maxDiff:
                    mjdDiffPass = True
                    break

    return mjdDiffPass

# 2016-02-21 KWS Quick code to check how many recurrences we have on each (whole) MJD.

def countIntraDayRecurrences(objectInfo):
    """Count the number of occurrences per whole MJD"""

    mjdCount = {}

    for row in objectInfo:
        try:
            mjdCount[int(row['MJD'])] += 1
        except KeyError as e:
            mjdCount[int(row['MJD'])] = 1

    return mjdCount

# 2016-02-21 KWS On how many MJDs do we have the minimum required recurrences?  E.g. We require
#            2 separate MJDs to have at least 3 recurrences.  This ensures that we don't simply
#            trigger an object when we have n detections.  We want to make sure the object was
#            detected in 3/4 of a quad on at least 2 separate days.

def checkIntraDayRecurrences(mjdCount, intraDayThreshold = INTRA_DAY_THRESH, numberOfDays = INTRA_DAY_THRESH_DAYS, mjdWindow = MJD_WINDOW):

    # 2016-02-25 KWS Only trigger if the day that contains the detections is within (e.g.) 5 days of the most recent day.
    maxMJD = sorted(mjdCount.keys())[-1]

    checkMJDRecurrences = False
    dayCount = 0
    triggerMJD = None
    for mjd, number in mjdCount.items():
        if number >= intraDayThreshold and mjd >= (maxMJD - mjdWindow):
            dayCount += 1
            if dayCount >= numberOfDays:
                checkMJDRecurrences = True
                triggerMJD = mjd
                break
                

#    if dayCount >= numberOfDays:
#        checkMJDRecurrences = True

    return checkMJDRecurrences, triggerMJD


def getObjectInfo(conn, objectId, mjdThreshold = None):
   """
   Get all object occurrences. Grab the quality data as well so we can make a subsequent
   decision to reject the recurrence if necessary.
   For ATLAS, the 'fpa_object' name is also the field name.
   """

   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      # Note that DEC is a MySQL reserved word, so need quotes around it

      # 2011-10-12 KWS Addded imageid to order by clause to make sure that the results
      #                are ordered consistently
      # 2013-08-26 KWS Addded flags so we can check for ghosts
      # 2018-03-16 KWS Pull out the x and y locations of a detection. If too close to the edge
      #                we can use this to delay detection flagging.
      # 2020-04-29 KWS Added ability to grab everything BEFORE a stated MJD so we can grab what
      #                detections would have been available on a certain date.
      # 2022-08-30 KWS Get the chi^2/n value as well so we can check it.
      if mjdThreshold is not None:
          cursor.execute ("""
                SELECT d.ra RA,
                       d.dec 'DEC',
                       m.filt Filter,
                       m.mjd MJD,
                       m.filename Filename,
                       m.obj field,
                       d.mag,
                       d.dmag,
                       m.texp exptime,
                       m.obs expname,
                       m.mag5sig,
                       m.obj object,
                       d.pmv,
                       d.pvr,
                       d.ptr,
                       d.pkn,
                       d.det,
                       d.dup,
                       d.psc,
                       d.pbn,
                       d.x,
                       d.y,
                       d.chin
                FROM atlas_detectionsddc d, atlas_metadataddc m
                where d.atlas_object_id=%s
                and d.atlas_metadata_id = m.id
                and m.mjd < %s
                ORDER by MJD
          """, (objectId, mjdThreshold))
      else:
          cursor.execute ("""
                SELECT d.ra RA,
                       d.dec 'DEC',
                       m.filt Filter,
                       m.mjd MJD,
                       m.filename Filename,
                       m.obj field,
                       d.mag,
                       d.dmag,
                       m.texp exptime,
                       m.obs expname,
                       m.mag5sig,
                       m.obj object,
                       d.pmv,
                       d.pvr,
                       d.ptr,
                       d.pkn,
                       d.det,
                       d.dup,
                       d.psc,
                       d.pbn,
                       d.x,
                       d.y,
                       d.chin
                FROM atlas_detectionsddc d, atlas_metadataddc m
                where d.atlas_object_id=%s
                and d.atlas_metadata_id = m.id
                ORDER by MJD
          """, (objectId,))
      result_set = cursor.fetchall ()

      cursor.close ()

   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))
      sys.exit (1)


   return result_set


def getObjectClassification(conn, objectId):
   """
   We would like to get the object classification for each object. Note that we are hitting the DB again
   becasue the original list was a list of Numpy numbers (required to save memory).
   """

   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      cursor.execute ("""
          select object_classification
            from atlas_diff_objects
           where id = %s
      """, (objectId,))
      resultSet = cursor.fetchone ()

      cursor.close ()

   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))
      sys.exit (1)

   return resultSet


def calcRMS(objectInfo, aveRa, aveDec, rms = None):
   sep = sepsq = 0

   for objectRow in objectInfo:
      delra = (aveRa-objectRow["RA"])*cos(aveDec * RADIANS)
      deldec = aveDec - objectRow["DEC"]
      delra *= 3600
      deldec *= 3600
      sep = sqrt(delra**2 + deldec**2)

      if rms:
         if sep < (2 * rms):
            sepsq = sepsq + delra**2 + deldec**2
      else:
         sepsq = sepsq + delra**2 + deldec**2
      
   rms = sqrt(sepsq/len(objectInfo))
   rms = round(rms, 3)
   return rms


def calculateRMSScatter(objectInfo):

   ### PRINT DETECTION INFORMATION & DETERMINE RMS SEPARATION FROM AVERAGE POSITION ###
   # 2017-10-30 KWS Set initial variables to zero, not equal to each other = 0.
   sep = 0
   totalRa = 0
   totalDec = 0
   sepsq = 0

   # Return negative RMS if no objects in the list (shouldn't happen)
   if len(objectInfo) == 0:
      return -1.0

   for objectRow in objectInfo:
      totalRa += objectRow["RA"]
      totalDec += objectRow["DEC"]

   aveRa = totalRa / len(objectInfo)
   aveDec = totalDec / len(objectInfo)

   #print "\taverage RA = %f, average DEC = %f" % (aveRa, aveDec)

   rms = calcRMS(objectInfo, aveRa, aveDec)

   ## APPLY 2-SIGMA CLIPPING TO THE RMS SCATTER -- TO REMOVE OUTLIERS (TWO ITERATIONS) ####

   rms = calcRMS(objectInfo, aveRa, aveDec, rms = rms)
   rms = calcRMS(objectInfo, aveRa, aveDec, rms = rms)

   return rms



def getMaxFollowupId (conn):

   followupId = 1

   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      cursor.execute ("""
          select max(followup_id) max_followup_id
          from atlas_diff_objects
          where followup_id is not null
      """ )

      resultSet = cursor.fetchall ()

      if cursor.rowcount > 0:
         if resultSet[0]['max_followup_id']:
            followupId = resultSet[0]['max_followup_id']

      cursor.close ()

   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))
      sys.exit (1)

   return followupId



def promoteObjectToEyeballList(conn, objectId, followupId, flagDate):
   """
   Update the object and add the followup ID.  This will be called by the master process in a
   single threaded manner.
   """

   rowsUpdated = 0
   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      cursor.execute ("""
         update atlas_diff_objects
            set followup_id = %s,
                detection_list_id = 4,
                followup_flag_date = %s
          where id = %s
            and detection_list_id is null
            and followup_id is null
            """, (followupId, flagDate, objectId))


   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))

   rowsUpdated = cursor.rowcount
   cursor.close ()

   return rowsUpdated


# 2021-09-28 KWS Added simple y = mx + c function for masking the corners of a faulty chip.
def fn(m, x, c):
    return m * x + c


# 2020-04-02 KWS Added flag date, in case we are doing a retrospective test on an object.
#                We flag objects within a window of 6 days, after which the object will
#                fail to be flagged.  The window stops old data from being flagged, but
#                we can't subsequently use this to retrospectively test an object for
#                promotion unless we also include the original flag date (converted
#                to MJD of course).
def testObject(conn, object, mjdWindow = MJD_WINDOW, debug = True, checkTriggerIsRecent = False, followupFlagDate = None, masks = None):

   siteMasks = {}
   # We shouldn't have no mask. Create one if necessary.
   if masks is None:
      for site in ['01a','02a','03a','04a']:
         siteMasks[site] = n.zeros((128,128), dtype=n.int)
      masks = siteMasks

   # Grab all info for each object, including the number of quality observations
   followupFlagMJD = None
   if followupFlagDate is not None:
       followupFlagMJD = getMJDFromSqlDate(followupFlagDate + ' 00:00:00') + 1
   objectInfo = getObjectInfo(conn, object, mjdThreshold = followupFlagMJD)

   # 2024-04-18 KWS Added the following check. If the database is in an inconsistent state (e.g.
   #                ingest crashed as detections were being inserted) then it's possible that there will
   #                be objects in the detections table that are NOT in the objects table (since it is
   #                inserted last). Hence objectInfo will be None. If it's None, record the bad object ID
   #                (so it can be later expunged from the database).
   if objectInfo is None:
       # We can't continue.
       print("Object %d is not in the objects table." % object)
       return None

   objectClassification = getFlagDefs(getObjectClassification(conn, object)['object_classification'], FLAGS, delimiter = ' ')

   # By default, we don't want to bother promoting the objects
   objectForUpdate = {'id': object, 'promote': False, 'classification': objectClassification, 'triggerMJD': None}

   # At the end of this function we want to return a list of all objects to promote
   # AND a list of all objects to flag as not having a TTI pair.  We should therefore
   # return a list of dicts rather than just a list of object IDs.

   # Count the "quality" detections and set the "QTP" flag.
   qtpCount = 0

   # For each object, check the individual detections. Are they of "good" quality?
   # For ATLAS, check that we have a magnitude, that the intrumental mag is set,
   # that the instrumental mag is set, that we have non-zero noise and the SNR is good
   # enough.
   detectionFlag = 1
   detectionList = []
   cleanedObjectInfo = []
   mjds = []
   for objectRow in objectInfo:
      #print objectRow
      # 2016-06-10 KWS Eliminate the Mount Model (MM) and test pointings
      # 2017-09-29 KWS New ddc cuts - must be flagged as either variable or transient,
      #                and must be positive flux, and must not be a known or suspected
      #                mover.
      # 2017-10-03 KWS Only pick up POSITIVE FLUX (det != 5).
      # 2018-03-16 KWS Don't flag an object as of interest if it's within 100 pixels of
      #                the edge of the chip. Genuinely real objects will get flagged
      #                later when the object lies in a different chip position.
      # 2018-04-26 KWS Don't flag an object if it's in MLO bad amplifier area.
      # 2018-11-05 KWS Don't flag an object if its 'dup' values are negative. And avoid 't' filter
      #                triggers for the time being.
      # 2019-08-27 KWS OK to flag MLO after 58718. Amplifier fixed.
      # 2020-04-02 KWS Belt & braces. Mag must be > 0 (should be same as det != 5). Also reduce
      #                pvr & ptr to just non-zero if psc > 0. (Catch those objects inside big
      #                galaxies that have a high psc value.
      # 2021-09-28 KWS Added masking for the corners of the "fuzzy" MLO chip which was reinstalled on
      #                September 10th 2021.  This mask will be removed as the calibration improves.
      # 2022-03-28 KWS Shutter problems on HKO creating problems with the subtractions. Mask out
      #                until problem is resolved.
      # 2022-04-22 KWS Removed HKO shutter mask - and not ('02a' in objectRow['expname'] and objectRow['x'] < CHIP_SIZE/4.0)
      # 2022-07-15 KWS Removed pmv check (and objectRow['pmv'] < PMV_THRESH). If it's a known mover it
      #                should be flagged by pkn. The pmv value seems to be set incorrectly too many times.
      # 2022-08-30 KWS Introduced a chi^2/n threshold. Hopefully this will remove egregiously non PSF like objects.
      # 2022-09-08 KWS Introduced chip masks.
      # 2022-09-16 KWS Removed superfluous  and not ('01a' in objectRow['expname'] and objectRow['x'] < CHIP_SIZE/8.0 and objectRow['y'] < CHIP_SIZE / 2.0 and objectRow["MJD"] < 58718)
      #                                     and not ('01a' in objectRow['expname'] and objectRow["MJD"] > 59467 and (objectRow['y'] - fn(1.4, objectRow['x'], 8500) > 0 or objectRow['y'] - fn(-1, objectRow['x'], 1200) < 0))
      # 2022-09-27 KWS Just can't do the ((objectRow['pvr'] >= 0 or objectRow['ptr'] >= 0) and objectRow['psc'] > PSC_THRESH) Too much junk coming through.
      #                Also got rid of objectRow['chin'] < CHIN_THRESH. Too unreliable.
      # 2023-02-24 KWS I've reinstated the above ((objectRow['pvr'] >= 0 or objectRow['ptr'] >= 0) and objectRow['psc'] > PSC_THRESH) condition.
      #                Since the heatmap is now in place for all detectors, this should mitigate agains the main sources of junk being used to
      #                trigger bogus objects.
      # 2023-03-01 KWS I've altered the above condition to ((objectRow['pvr'] >= 0 or objectRow['ptr'] >= 0) and (objectRow['psc'] > PSC_THRESH and objectRow['psc'] < 900)) condition.

      binnedx = int(objectRow['x']*(masks[objectRow['expname'][0:3]].shape[1] - 1)/(CHIP_SIZE-1))
      binnedy = int(objectRow['y']*(masks[objectRow['expname'][0:3]].shape[0] - 1)/(CHIP_SIZE-1))

      if binnedx < 0:
          binnedx = 0

      if binnedx > (masks[objectRow['expname'][0:3]].shape[1] - 1):
          binnedx = masks[objectRow['expname'][0:3]].shape[1] - 1

      if binnedy < 0:
          binnedy = 0

      if binnedy > (masks[objectRow['expname'][0:3]].shape[0] - 1):
          binnedy = masks[objectRow['expname'][0:3]].shape[0] - 1

      detInMaskArea = masks[objectRow['expname'][0:3]][binnedy][binnedx]
      #print detInMaskArea
      #print int(objectRow['y']*masks[objectRow['expname'][0:3]].shape[0]/(CHIP_SIZE-1)), int(objectRow['x']*masks[objectRow['expname'][0:3]].shape[1]/(CHIP_SIZE-1))
      #testMask = masks[objectRow['expname'][0:3]].copy()
      #testMask[int(objectRow['y']*masks[objectRow['expname'][0:3]].shape[0]/(CHIP_SIZE-1))][int(objectRow['x']*masks[objectRow['expname'][0:3]].shape[1]/(CHIP_SIZE-1))] = 3
      #testMask[binnedy][binnedx] = 3
      #print
      #print n.flip(testMask, 0)
      #print

      if 'MM' not in objectRow['object'] \
              and 'test' not in objectRow['object'] \
              and objectRow['pkn'] < PMV_THRESH \
              and ((objectRow['pvr'] >= PVR_THRESH or objectRow['ptr'] >= PTR_THRESH) or ((objectRow['pvr'] > 0 or objectRow['ptr'] > 0) and objectRow['psc'] > 0) or ((objectRow['pvr'] >= 0 or objectRow['ptr'] >= 0) and (objectRow['psc'] > PSC_THRESH and objectRow['psc'] < 900))) \
              and objectRow['det'] != 5 \
              and objectRow['dup'] >= 0 \
              and objectRow['mag'] > 0 \
              and objectRow['pbn'] < PBN_THRESH \
              and objectRow['Filter'] != 't' \
              and objectRow['x'] > MIN_SIZE \
              and objectRow['x'] < MAX_SIZE \
              and objectRow['y'] > MIN_SIZE \
              and objectRow['y'] < MAX_SIZE \
              and not detInMaskArea:

          mjds.append([objectRow["MJD"]])
          detectionList.append((objectRow, detectionFlag))
          cleanedObjectInfo.append(objectRow)
          qtpCount += 1
      else:
          pass
          #print objectRow

   if qtpCount >= RECURRENCE_THRESH:

      if debug:
         print("")
         print("======")
         print("Candidate ID = %s (%s). QTPCount = %d." % (object, objectClassification, qtpCount))


      passesMJDCheck = checkDeltas(mjds, RECURRENCE_THRESH, MIN_MJD_DIFF, mjddiffmax = INTRA_DAY_THRESH_DAYS/2.0, mjdWindow = MJD_WINDOW, followupFlagDate = followupFlagDate)
      if passesMJDCheck:
         if debug:
            print("\tMinimum MJD gap is acceptable (pass)")
      else:
         if debug:
            print("\tMinimum MJD gap not large enough (fail)")

      rms = calculateRMSScatter(cleanedObjectInfo)
      if rms > RMS_SCATTER_THRESH:
         if debug:
            print("\trms position scatter = %f (too high - fail)" % rms)
      else:
         if debug:
            print("\trms position scatter = %f (pass)" % rms)

      # 2016-02-21 KWS We MUST have at least 2 days with 3 recurrences
      mjdCount = countIntraDayRecurrences(cleanedObjectInfo)
      passesMJDCountCheck, triggerMJD = checkIntraDayRecurrences(mjdCount, mjdWindow = mjdWindow)
      if checkTriggerIsRecent:
         # Check that the trigger is within the last mjdWindow*2 days.
         mjd = getCurrentMJD()
         if followupFlagMJD is not None:
            mjd = followupFlagMJD
         if triggerMJD < mjd - mjdWindow*2:
            print("\tTrigger MJD is not recent (fail)")
            passesMJDCountCheck = False

      if passesMJDCountCheck:
         if debug:
            print("\tMJD count is acceptable (pass)")
            print("\tTrigger MJD = %d" % triggerMJD)
      else:
         if debug:
            print("\tMJD count not acceptable (fail)")

      # If we have more than the required intra-day detections, increase RMS threshold.


      ### PRINT DETECTION ARRAY ###
      if debug:
         print("\tdetection array = ", end=' ')
         for detectionRow in detectionList:
            sys.stdout.write("%d" % detectionRow[1])

         print("")

         ### PRINT FILTER ARRAY ###
         print("\t   filter array = ", end=' ')
         for detectionRow in detectionList:
            sys.stdout.write("%s" % detectionRow[0]["Filter"])

         print("")

      # Flags - will replace later with boolean values
      windowDetect = 0
      filterSwitch = 0

      # 2013-04-24 KWS The initial value of detectionRowIndex must be 1, not 0.
      detectionRowIndex = 1
      for detectionRow in detectionList:
         obsFilters = []
         detectionSwitch = 0 # detection detectionSwitch reset
         windowStart = detectionRowIndex - WINDOW_SIZE

         if windowStart < 0:
            windowStart = 0


         # Loop over list slices.

         for innerDetectionRow in detectionList[windowStart:detectionRowIndex]:
            detectionSwitch += innerDetectionRow[1]
            if innerDetectionRow[1] == 1:
               obsFilters.append(innerDetectionRow[0]["Filter"])

         if detectionSwitch >= WINDOW_DETECTIONS_THRESH:
            windowDetect = 1

            # Now go through the observed filters.  Count them. Note that this code currently checks
            # for ANY observation in any filter.  We should really be looking for any QUALITY detection
            # in any filter

            filterDict = {}
            for filter in obsFilters:
               filterDict[filter] = 1

               filterList = list(filterDict.keys())

               if len(filterList) >= FILTERS_THRESH:
                  # We have the required number of filters.  No need to continue.
                  filterSwitch = 1
                  break

         detectionRowIndex += 1


      if debug:
         print("\t%s --> " % object, end=' ')
         ## PRINT RESULTS
         if rms < RMS_SCATTER_THRESH:
            print("RMS : 1,", end=' ')
         else:
            print("RMS : 0,", end=' ')
   
         if windowDetect == 0:
            print(" DETECTIONS : 0,", end=' ')
         else:
            print(" DETECTIONS : 1,", end=' ')
   
         if filterSwitch == 0:
            print(" FILTERS : 0,", end=' ')
         else:
            print(" FILTERS : 1,", end=' ')

         if passesMJDCheck:
            print(" MJDGAP : 1", end=' ')
         else:
            print(" MJDGAP : 0", end=' ')

      if rms < RMS_SCATTER_THRESH and windowDetect == 1 and filterSwitch and passesMJDCheck and passesMJDCountCheck:
         if debug:
            print(" ---> passed threshold cuts")
         # Add the object to the list of object for update.
         objectForUpdate["promote"] = True
         objectForUpdate["triggerMJD"] = triggerMJD
         #objectsPromoted += 1
      else:
         if debug:
            print(" ---> failed threshold cuts")

   return objectForUpdate

 




def applyAtlasDiffCuts(conn, dateTime, objectList, recent = False, flagDate = None, masks = None):
   """
   Main program to apply the cuts. This routine should be called by the worker function,
   which also grabs a subset of the objects (could be all of them) to check.
   The worker function will then add the objects to the queue for readout by the master.
   """

   # No need to shove them into an array. Let's save time by just iterating over the results array.

   siteMasks = {}
   if masks is None:
      for site in ['01a','02a','03a','04a']:
         siteMasks[site] = n.zeros((128,128), dtype=n.int)
      masks = siteMasks

   objectListForUpdate = []

   objectsPromoted = 0

   for object in objectList:
      objectForUpdate = testObject(conn, object, checkTriggerIsRecent = recent, followupFlagDate = flagDate, masks = masks)

      # 2024-04-18 KWS objectForUpdate should never be None unless we've had a database crash.
      if objectForUpdate is not None:
         if objectForUpdate["promote"]:
            objectsPromoted += 1

         objectListForUpdate.append(objectForUpdate)
 
   print("")
   print("TOTAL = %d" % len(objectList))
   print("GOOD = %d" % objectsPromoted)

   return objectListForUpdate


def test():
   log = logging.getLogger(__name__)
   log.info('BARF!!')




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

    update = options.update
    recent = options.recent

    conn = dbConnect(hostname, username, password, database, quitOnError = True)

    # 2023-03-13 KWS MySQLdb disables autocommit by default. Switch it on globally.
    conn.autocommit(True)

    objectList = []

    dateThreshold = None

    currentDate = datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")
    (year, month, day, hour, min, sec) = currentDate.split(':')
    followupFlagDate = "%s-%s-%s %s:%s:%s" % (year, month, day, hour, min, sec)
    dateTime = "%s-%s-%s %s:%s:%s" % (year, month, day, hour, min, sec)
    sqlFlagDate = "%s-%s-%s" % (year, month, day)

    if len(options.candidate) > 0:
        for candidate in options.candidate:
            try:
                objectList.append(candidate)
            except ValueError:
                print("Object IDs must be integers")
                sys.exit(1)

    else:
        # Get only the ATLAS objects that don't have the 'moons' flag set.
        dateThreshold = getMostRecentProcessedDate(conn)

        # When both thresholds are defined, use the one in the database.
        if options.datethreshold is not None and dateThreshold is not None:
            pass
        else:
            try:
                dateThreshold = '%s-%s-%s 00:00:00' % (options.datethreshold[0:4], options.datethreshold[4:6], options.datethreshold[6:8])
            except:
                # Something went wrong parsing the datethreshold - just use the start of today.
                dateThreshold = sqlFlagDate

        print(dateThreshold, sqlFlagDate)

        objectList = getAtlasObjectsToCheck(conn, dateThreshold = dateThreshold, filterObjectsAlreadyPromoted = True)


    print("Length of list is:", len(objectList))

    objectListForUpdate = []

    # 2022-09-07 KWS Read the heatmaps for each site.
    multiplier = 1.5
    siteMasks = getSiteMasks(conn, multiplier)

    objectListForUpdate = applyAtlasDiffCuts(conn, dateTime, objectList, recent = recent, masks = siteMasks)

    # Child process needs to write this list onto its Queue.
    # Master process will iterate through the combined Queues.
    if len(objectListForUpdate) > 0:
        print("")
        print("%d objects for update" % len(objectListForUpdate))
        for object in objectListForUpdate:
            print(object)

        if update:

            # Grab highest followup ID.  MASTER PROCESS
            maxFollowupId = getMaxFollowupId(conn)

            print("")
            print("Max followup ID = %d" % maxFollowupId)

            objectsPromoted = 0
            followupId = maxFollowupId + 1

            for object in objectListForUpdate:
                if object["promote"]:
                    promoteObjectToEyeballList(conn, object["id"], followupId, sqlFlagDate)
                    followupId += 1
                    objectsPromoted += 1

            print("TOTAL OBJECTS PROMOTED = %d" % objectsPromoted)

    else:
        print("")
        print("No objects to update.")



    conn.commit ()
    conn.close ()

    return


# ###########################################################################################
#                                         Main hook
# ###########################################################################################


if __name__=="__main__":
     main()

