#!/usr/bin/env python
"""Generate and load up the stacked forced photometry for this object.

Usage:
  %s <configfile> [<candidate>...] [--detectionlist=<detectionlist>] [--customlist=<customlist>] [--update] [--days=<days>] [--mjdmin=<mjdmin>] [--mjdmax=<mjdmax>]
  %s (-h | --help)
  %s --version

Options:
  -h --help                         Show this screen.
  --version                         Show version.
  --update                          Update the database.
  --detectionlist=<detectionlist>   Object detection list [default: 4].
  --customlist=<customlist>         Object custom list.
  --days=<days>                     Max number of days to go back before flag date or now [default: 30].
  --mjdmin=<mjdmin>                 Min MJD (overrides Max number of days above).
  --mjdmax=<mjdmax>                 Max MJD (overrides Max number of days above).

  Example:
    %s ../../../../../atlas/config/config4_db5_readonly.yaml 1161021541115506200 --update
"""
import sys
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt
import os, MySQLdb, shutil, re
from gkutils.commonutils import find, Struct, cleanOptions, dbConnect, readGenericDataFile, calculateRMSScatter, getCurrentMJD, getMJDFromSqlDate
from makeATLASStamps import getObjectsByList, getObjectsByCustomList
from getATLASForcedPhotometry import getATLASObject
import MySQLdb
import subprocess
import io

remoteServer='atlas-base-sc01.ifa.hawaii.edu'
remoteUser='ksmith'

def getObjectInfoddc(conn, objectId, negativeFlux = False):
   """
   Get all object occurrences.
   """

   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      if negativeFlux:
          cursor.execute ("""
                SELECT d.ra RA,
                       d.dec 'DEC',
                       d.atlas_object_id,
                       m.filt Filter,
                       m.mjd MJD,
                       m.filename Filename,
                       m.obj field,
                       d.mag,
                       d.dmag dm,
                       m.texp exptime,
                       m.obs expname,
                       m.mag5sig,
                       d.det_id,
                       d.pmv,
                       d.pvr,
                       d.ptr,
                       d.pkn,
                       d.det,
                       d.dup,
                       d.id,
                       d.x,
                       d.y,
                       d.htm16ID
                FROM atlas_detectionsddc d, atlas_metadataddc m
                where d.atlas_object_id=%s
                and d.atlas_metadata_id = m.id
                ORDER by MJD
          """, (objectId,))
      else:
          cursor.execute ("""
                SELECT d.ra RA,
                       d.dec 'DEC',
                       d.atlas_object_id,
                       m.filt Filter,
                       m.mjd MJD,
                       m.filename Filename,
                       m.obj field,
                       d.mag,
                       d.dmag dm,
                       m.texp exptime,
                       m.obs expname,
                       m.mag5sig,
                       d.det_id,
                       d.pmv,
                       d.pvr,
                       d.ptr,
                       d.pkn,
                       d.det,
                       d.dup,
                       d.id,
                       d.x,
                       d.y,
                       d.htm16ID
                FROM atlas_detectionsddc d, atlas_metadataddc m
                where d.atlas_object_id=%s
                and d.atlas_metadata_id = m.id
                and d.det != 5
                and d.mag > 0.0
                and (d.deprecated != 1 or d.deprecated is null)
                ORDER by MJD
          """, (objectId,))
      resultSet = cursor.fetchall ()

      cursor.close ()

   except MySQLdb.Error as e:
      print(str(e))
      sys.exit (1)


   return resultSet

def getMaxStackedPhotometryMJD(conn, candidate):
   """
   Get the max exiting MJD for the stacked photometry, if it exists.
   """

   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      cursor.execute ("""
            SELECT max(mjd) maxMJD
              FROM atlas_stacked_forced_photometry
             WHERE atlas_object_id=%s
      """, (candidate.id,))
      resultSet = cursor.fetchone ()

      cursor.close ()

   except MySQLdb.Error as e:
      print(str(e))
      sys.exit (1)


   return resultSet


def insertStackDetRow(conn, candidate, row):
    """
    Add the row to the database

    :param conn: database connection
    :param row:
    :return: insert id

    """
    import MySQLdb

    try:
        cursor = conn.cursor (MySQLdb.cursors.DictCursor)

        cursor.execute ("""
             insert into atlas_stacked_forced_photometry
                        (atlas_object_id,
                         mjd,
                         m,
                         dm,
                         ujy,
                         dujy,
                         f,
                         err,
                         chin,
                         ra,
                         `dec`,
                         x,
                         y,
                         maj,
                         min,
                         phi,
                         apfit,
                         sky,
                         zp,
                         stack,
                         date_inserted,
                         redo_photometry)
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), %s)
                  """, ( candidate,
                         row['##MJD'],
                         row['m'],
                         row['dm'],
                         row['uJy'],
                         row['duJy'],
                         row['F'],
                         row['err'],
                         row['chi/N'],
                         row['RA'],
                         row['Dec'],
                         row['x'],
                         row['y'],
                         row['maj'],
                         row['min'],
                         row['phi'],
                         row['apfit'],
                         row['Sky'],
                         row['ZP'],
                         row['Stack'],
                         0))

    except MySQLdb.Error as e:
        sys.stderr.write("Error %d: %s\n" % (str(e)))

    cursor.close ()
    return conn.insert_id()

def runStackForced(conn, candidate, mjdMin = 50000, mjdMax = 70000):
    photometry = []
    p = subprocess.Popen(["ssh", "%s@%s" % (remoteUser, remoteServer), "sforce.sh", str(candidate.ra), str(candidate.dec),   "outdir=/home/ksmith/sforce", "m0=%f" % mjdMin, "m1=%f" % mjdMax,  "id=%d mkmontage=0 mkplot=0" % candidate.id], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, errors = p.communicate()

    if output.strip():
        pass
    if errors.strip():
        print(errors)
    
    # Now get the result
    p = subprocess.Popen(["scp -p %s@%s:/home/%s/sforce/%s.sforce ." % (remoteUser, remoteServer, remoteUser, candidate.id)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    output, errors = p.communicate()

    if output.strip():
        pass
    if errors.strip():
        print(errors)
    
    photometry = readGenericDataFile("%s.sforce" % candidate.id, delimiter=' ')

    return photometry


def doStackedForcedPhotometry(conn, options, objectList):
    class candidate:
        pass

    data = []

    for c in objectList:
        lc = getObjectInfoddc(conn, c['id'])

        ra_avg, dec_avg, rms = calculateRMSScatter(lc)
        candidate.id = c['id']
        candidate.ra = ra_avg
        candidate.dec = dec_avg

        print(candidate.id, candidate.ra, candidate.dec)

        mjdMin = 50000
        today = getCurrentMJD()
        followupFlagMJD = None

        if c['followup_flag_date']:
            followupFlagMJD = getMJDFromSqlDate(c['followup_flag_date'].strftime("%Y-%m-%d") + ' 00:00:00')

        if options.days and followupFlagMJD is not None:
            mjdMin = followupFlagMJD - float(options.days)

        if options.days and followupFlagMJD is None:
            mjdMin = today - float(options.days)

        if options.mjdmin:
            mjdMin = float(options.mjdmin)

        mjdThreshold = getMaxStackedPhotometryMJD(conn, candidate)
        if mjdThreshold['maxMJD'] is not None:
            mjdMin = int(mjdThreshold['maxMJD']) + 1
        photometry = runStackForced(conn, candidate, mjdMin = mjdMin)
        if photometry:
            data.append({'id': candidate.id, 'photometry': photometry})

    return data



def main(argv = None):
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

    conn = dbConnect(hostname, username, password, database, quitOnError = True)
    conn.autocommit(True)

    objectList = []

    if options.candidate is not None and len(options.candidate) > 0:
        for cand in options.candidate:
            obj = getATLASObject(conn, objectId = int(cand))
            if obj:
                objectList.append(obj)
    else:

        if options.customlist is not None:
            if int(options.customlist) > 0 and int(options.customlist) < 100:
                customList = int(options.customlist)
                objectList = getObjectsByCustomList(conn, customList, processingFlags = 0)
            else:
                print("The list must be between 1 and 100 inclusive.  Exiting.")
                sys.exit(1)
        else:
            if options.detectionlist is not None:
                if int(options.detectionlist) >= 0 and int(options.detectionlist) <= 11:
                    detectionList = int(options.detectionlist)
                    objectList = getObjectsByList(conn, listId = detectionList, dateThreshold = flagDate, processingFlags = 0)
                else:
                    print("The list must be between 0 and 11 inclusive.  Exiting.")
                    sys.exit(1)

    if len(objectList) > 0:
        data = doStackedForcedPhotometry(conn, options, objectList)
        if len(data) > 0 and options.update:
            for row in data:
                for p in row['photometry']:
                    print (row['id'], p)
                    insertStackDetRow(conn, row['id'], p)

    conn.commit()

    return


if __name__ == '__main__':
    main()
