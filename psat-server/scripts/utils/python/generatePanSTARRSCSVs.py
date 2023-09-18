#!/usr/bin/env python
"""Generate CSV summary and recurrence files for (e.g.) PESSTO and YSE.

Usage:
  %s <configfile> [--lists=<lists>] [--delimiter=<delimiter>] [--customlist=<customlist>] [--writeAGNs] [--summaryfile=<summaryfile>] [--recfile=<recfile>] [--agnsummaryfile=<agnsummaryfile>] [--agnrecfile=<agnrecfile>]
  %s (-h | --help)
  %s --version

Options:
  -h --help                          Show this screen.
  --version                          Show version.
  --lists=<lists>                    Lists to generate information for [default: 1,2,3].
  --listAGN=<listAGN>                AGN list [default: 7].
  --delimiter=<delimiter>            Delimiter [default: |].
  --customlist=<customlist>          Custom list.
  --writeAGNs                        Write the AGNS.
  --summaryfile=<summaryfile>        Summary filename [genericsummary.csv].
  --recfile=<recfile>                Recurrences filename [genericrecurrences.csv].
  --agnsummaryfile=<agnsummaryfile>  Summary filename for AGNs if required [agnsummary.csv].
  --agnrecfile=<agnrecfile>          AGN recurrences filename if required [agnrecurrences.csv].

E.g.:
  %s ../../../../../ps13pi/config/config.yaml --lists=1,2,3 --writeAGNs
"""

import sys
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0])

from docopt import docopt
import sys, os, shutil, re, csv
from gkutils.commonutils import dbConnect, Struct, cleanOptions, calculateHeatMap

import warnings
warnings.filterwarnings('ignore', '.*the sets module is deprecated.*', DeprecationWarning, 'MySQLdb')
import sys, os, MySQLdb, csv
from gkutils.commonutils import coords_dec_to_sex, ra_to_sex, dec_to_sex, getFlagDefs

SUMMARY_CSV_FILENAME = 'summary.csv'
RECURRENCES_CSV_FILENAME = 'recurrences.csv'

SUMMARY_CSV_FILENAME_GENERIC = 'genericsummary.csv'
RECURRENCES_CSV_FILENAME_GENERIC = 'genericrecurrences.csv'

SUMMARY_CSV_FILENAME_AGN = 'agnsummary.csv'
RECURRENCES_CSV_FILENAME_AGN = 'agnrecurrences.csv'

FILE_ROOT = '/' + os.uname()[1].split('.')[0] + '/images'

FLAGS = {'orphan':         1,
         'variablestar':   2,
         'nt':             4,
         'agn':            8,
         'sn':            16,
         'miscellaneous': 32,
         'tde':           64,
         'lens':         128,
         'mover':        256}

ALL_OBJECTS = 1
EXCLUDE_AGNS = 2
AGNS_ONLY = 3

QUBLISTS = {
   0:"GARBAGE",
   1:"CONFIRMED",
   2:"GOOD",
   3:"POSSIBLE",
   4:"EYEBALL",
   5:"ATTIC",
   6:"ZOO",
   7:"AGNS",
   None:"UNLISTED"
}


# Get the recurrence data.  This query is similar to the one currently run to generate the web page lightcurve data.
# The web pages should be running this code, but for the time being we will run this as a script

def getSummaryData(conn, dateThreshold, customList = 2):
   """getSummaryData.

   Args:
       conn:
       dateThreshold:
       customList:
   """

   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      cursor.execute ('''
          select o.id, o.ra_psf, o.dec_psf, o.psf_inst_mag_sig, o.cal_psf_mag, substr(m.fpa_filter,1,1) filter, o.local_designation, o.ps1_designation, o.followup_flag_date, o.object_classification
            from tcs_transient_objects o, tcs_cmf_metadata m, tcs_object_groups og
           where o.tcs_cmf_metadata_id = m.id
             and o.id = og.transient_object_id
             and og.object_group_id = %s
             and o.followup_flag_date >= %s
             and o.ps1_designation is not null
      ''', (customList, dateThreshold))


      resultSet = cursor.fetchall ()
      cursor.close ()

   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))
      sys.exit (1)

   return resultSet


# 2015-08-07 KWS For the time being dump ALL Good and Confirmed objects into the Marshall

def getAllSummaryDataForGoodAndConfirmedObjects(conn, dateThreshold = '2013-06-01'):
   """getAllSummaryDataForGoodAndConfirmedObjects.

   Args:
       conn:
       dateThreshold:
   """

   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      cursor.execute ('''
          select o.id, o.ra_psf, o.dec_psf, o.psf_inst_mag_sig, o.cal_psf_mag, substr(m.fpa_filter,1,1) filter, o.local_designation, o.ps1_designation, o.followup_flag_date, o.object_classification
            from tcs_transient_objects o, tcs_cmf_metadata m
           where o.tcs_cmf_metadata_id = m.id
             and (o.detection_list_id = 1 or o.detection_list_id = 2)
             and o.followup_flag_date >= %s
             and o.ps1_designation is not null
      ''', (dateThreshold,))

      resultSet = cursor.fetchall ()
      cursor.close ()

   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))
      sys.exit (1)

   return resultSet


# 2013-10-01 KWS Needed to add 3pi diff filename def (RINGS...) to query below

def getGenericSummaryData(conn, detectionLists=[1,2,3,5], queryType = ALL_OBJECTS):
   """getGenericSummaryData.

   Args:
       conn:
       detectionLists:
       queryType:
   """

   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      sqlSelect = '''
          select o.id, o.ra_psf, o.dec_psf, o.psf_inst_mag_sig, o.cal_psf_mag, substr(m.fpa_filter,1,1) filter, o.local_designation, o.ps1_designation, o.other_designation tns_name, o.followup_flag_date, o.sherlockClassification, o.detection_list_id, sh.z sherlock_specz, sh.catalogue_object_id sherlock_object_id, sh.raDeg sherlock_host_ra, sh.decDeg sherlock_host_dec, ifnull(sh.direct_distance, sh.distance) sherlock_distancempc, sh.catalogue_table_name sherlock_tables, classification_confidence rb_factor_catalogue, confidence_factor rb_factor_image, s.latest_mjd_forced, s.latest_flux_forced, s.latest_dflux_forced, s.latest_filter_forced, s.latest_pscamera_forced
            from tcs_cmf_metadata m, tcs_latest_object_stats s
            join tcs_transient_objects o on s.id = o.id
       left join sherlock_crossmatches sh on sh.transient_object_id = o.id
           where o.tcs_cmf_metadata_id = m.id
             and (m.filename like 'MD%%%%' or m.filename like 'FGSS%%%%' or m.filename like 'RINGS%%%%')
             and ((sh.rank is null and sh.transient_object_id is null) or (sh.rank = 1 and sh.transient_object_id is not null))
      '''

      if queryType == EXCLUDE_AGNS:
         sqlSelect += ''' 
                         and o.sherlockClassification != 'AGN'
                      '''
      elif queryType == AGNS_ONLY:
         sqlSelect += '''
                         and o.sherlockClassification = 'AGN'
                      '''


      # Note - need 4 percent signs above because we will pass the result for another character insertion on cursor.execute

      sqlInPhrase = '''
             and o.detection_list_id in (%s)
        order by o.followup_flag_date desc
      '''

      in_p=', '.join(['%s' for x in detectionLists])

      sql = sqlSelect + sqlInPhrase % in_p

      cursor.execute(sql, detectionLists)

      resultSet = cursor.fetchall ()
      cursor.close ()

   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))
      sys.exit (1)

   return resultSet



def getAverageRAandDec(conn, id):
   """getAverageRAandDec.

   Args:
       conn:
       id:
   """

   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      cursor.execute ('''
          select avg(ra_psf) ra_psf, avg(dec_psf) dec_psf from (
              select ra_psf, dec_psf from tcs_transient_objects
              where id = %s
              union all
              select ra_psf, dec_psf from tcs_transient_reobservations
              where transient_object_id = %s) temp
      ''', (id, id))


      resultSet = cursor.fetchone ()
      cursor.close ()

   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))
      sys.exit (1)

   return resultSet


# 2013-10-02 KWS Now that we use most recent images for quickview, get the row here.
def getRepresentativeTargetImage(conn, id):
   """Return all image groups for an object.  We can decide later which one to use"""

   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      cursor.execute ('''
          select target, ref, diff
            from tcs_transient_objects o, tcs_images i
           where o.tcs_images_id = i.id
             and o.id = %s
      ''', (id,))


      resultSet = cursor.fetchone ()
      cursor.close ()

   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))
      sys.exit (1)

   return resultSet


# 2013-10-02 KWS Get target, ref, diff images - not just target
def getRecurrenceData(conn, id):
   """getRecurrenceData.

   Args:
       conn:
       id:
   """

   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      cursor.execute ('''
          select o.id,
                 o.id transient_object_id,
                 o.local_designation,
                 o.ps1_designation,
                 o.other_designation tns_name,
                 m.mjd_obs,
                 o.ra_psf,
                 o.dec_psf,
                 o.psf_inst_mag_sig,
                 o.ap_mag,
                 o.cal_psf_mag,
                 substr(m.fpa_filter,1,1)
                 filter,
                 ifnull(NULL, concat(name, '_target')) target,
                 ifnull(NULL, concat(name, '_ref')) ref,
                 ifnull(NULL, concat(name, '_diff')) diff,
                 m.fpa_detector pscamera
            from tcs_transient_objects o
      inner join tcs_cmf_metadata m
              on (o.tcs_cmf_metadata_id = m.id)
       left join tcs_image_groups i
              on (o.image_group_id = i.id)
           where o.id = %s
           union all
          select r.id,
                 r.transient_object_id,
                 o.local_designation,
                 o.ps1_designation,
                 o.other_designation tns_name,
                 m.mjd_obs,
                 r.ra_psf,
                 r.dec_psf,
                 r.psf_inst_mag_sig,
                 r.ap_mag,
                 r.cal_psf_mag,
                 substr(m.fpa_filter,1,1)
                 filter,
                 ifnull(NULL, concat(name, '_target')) target,
                 ifnull(NULL, concat(name, '_ref')) ref,
                 ifnull(NULL, concat(name, '_diff')) diff,
                 m.fpa_detector pscamera
            from tcs_transient_reobservations r
      inner join tcs_cmf_metadata m
              on (r.tcs_cmf_metadata_id = m.id)
      inner join tcs_transient_objects o
              on (r.transient_object_id = o.id)
       left join tcs_image_groups i
              on (r.image_group_id = i.id)
           where r.transient_object_id = %s
        order by mjd_obs desc
      ''', (id, id))

      resultSet = cursor.fetchall ()
      cursor.close ()

   except MySQLdb.Error as e:
      print("Error %d: %s" % (e.args[0], e.args[1]))
      sys.exit (1)

   return resultSet

# 2013-10

def producePESSTOCSV(conn, options, delimiter, customList, listId = 3, summaryCSVFilename = None, recurrenceCSVFilename = None):
   """producePESSTOCSV.

   Args:
       conn:
       database:
       delimiter:
       customList:
       listId:
   """
   import datetime

   currentDate = datetime.datetime.now().strftime("%Y-%m-%d")
   #dateThreshold = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
   # Need to subtract 7 days from the date and send this to the object list grabber.

   # 2013-10-02 KWS For the time being the date threshold is 1st June (the date the survey started).
   # 2014-12-09 KWS I've reset the date threshold to be 1st December, when the survey restarted.
   dateThreshold = '2014-12-01'

   #objectList = getSummaryData(conn, dateThreshold, customList = customList)
   objectList = getAllSummaryDataForGoodAndConfirmedObjects(conn)

   summaryCSVFile = open(summaryCSVFilename, 'w')

   recurrenceCSVFile = open(recurrenceCSVFilename, 'w')

   writeRecurrenceHeaderFlag = True

   if objectList:
      # Create the summary header, but also add the 'name' value we're creating for the representative image
      # and also the 'mjd_obs' of the earliest point
      summaryHeader = list(objectList[0].keys())
      summaryHeader.append('target')
      summaryHeader.append('ref')
      summaryHeader.append('diff')
      summaryHeader.append('mjd_obs')

      summarywh = csv.writer(summaryCSVFile, delimiter=delimiter)
      summarywh.writerow(summaryHeader)
      summaryw = csv.DictWriter(summaryCSVFile, fieldnames=summaryHeader, delimiter=delimiter)

      for row in objectList:
         # Replace the ra_psf and dec_psf values with the average ones for this transient
         coords = getAverageRAandDec(conn, row["id"])
         row["ra_psf"] = coords["ra_psf"]
         row["dec_psf"] = coords["dec_psf"]

         # Get a representative image
         triplet = getRepresentativeTargetImage(conn, row["id"])
         if triplet:
            row["target"] = triplet["target"] 
            row["ref"] = triplet["ref"] 
            row["diff"] = triplet["diff"] 
         else:
            row["target"] = None
            row["ref"] = None 
            row["diff"] = None 

         recurrenceList = getRecurrenceData(conn, row["id"])
         # Replace the cal_psf_mag with the actual earliest value from the recurrence list

         row["mjd_obs"] = recurrenceList[0]["mjd_obs"]
         row["cal_psf_mag"] = recurrenceList[0]["cal_psf_mag"]
         row["psf_inst_mag_sig"] = recurrenceList[0]["psf_inst_mag_sig"]
         row["filter"] = recurrenceList[0]["filter"]

         summaryw.writerow(row)

         # There will ALWAYS be at least one recurrence, so recurrenceList always present.
         recurrenceHeader = list(recurrenceList[0].keys())
         recurrencewh = csv.writer(recurrenceCSVFile, delimiter=delimiter)
         if writeRecurrenceHeaderFlag:
            recurrencewh.writerow(recurrenceHeader)
            writeRecurrenceHeaderFlag = False

         recurrencew = csv.DictWriter(recurrenceCSVFile, fieldnames=recurrenceHeader, delimiter=delimiter)

         for recRow in recurrenceList:
            recurrencew.writerow(recRow)



   summaryCSVFile.close()
   recurrenceCSVFile.close()

   return 0


def produceGenericCSV(conn, options, delimiter, detectionLists=[1,2,3,5], summaryCSVFilename = None, recurrenceCSVFilename = None, queryType = ALL_OBJECTS):
    """produceGenericCSV.

    Args:
        conn:
        database:
        delimiter:
        detectionLists:
        summaryCSVFilename:
        recurrenceCSVFilename:
        queryType:
    """
    import datetime

    currentDate = datetime.datetime.now().strftime("%Y-%m-%d")
    #dateThreshold = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
    # Need to subtract 7 days from the date and send this to the object list grabber.

    # 2013-10-02 KWS For the time being the date threshold is 1st June (the date the survey started).
    dateThreshold = '2013-06-01'

    # Need to subtract 1 month from the date and send this to the object list grabber.
    objectList = getGenericSummaryData(conn, detectionLists=detectionLists, queryType = queryType)


    summaryCSVFile = open(summaryCSVFilename, 'w')

    recurrenceCSVFile = open(recurrenceCSVFilename, 'w')

    writeRecurrenceHeaderFlag = True

    if objectList:
        # Create the summary header, but also add the 'name' value we're creating for the representative image
        # and also the 'mjd_obs' of the earliest point
        summaryHeader = list(objectList[0].keys())
        summaryHeader.append('target')
        summaryHeader.append('ref')
        summaryHeader.append('diff')
        summaryHeader.append('mjd_obs')

        summarywh = csv.writer(summaryCSVFile, delimiter=delimiter)
        summarywh.writerow(summaryHeader)
        summaryw = csv.DictWriter(summaryCSVFile, fieldnames=summaryHeader, delimiter=delimiter)

        for row in objectList:
            # Replace the ra_psf and dec_psf values with the average ones for this transient
            coords = getAverageRAandDec(conn, row["id"])
            row["ra_psf"] = coords["ra_psf"]
            row["dec_psf"] = coords["dec_psf"]

            # Get a representative image
            triplet = getRepresentativeTargetImage(conn, row["id"])
            if triplet:
                row["target"] = triplet["target"]
                row["ref"] = triplet["ref"]
                row["diff"] = triplet["diff"]
            else:
                row["target"] = None
                row["ref"] = None
                row["diff"] = None

            recurrenceList = getRecurrenceData(conn, row["id"])

            row["mjd_obs"] = recurrenceList[0]["mjd_obs"]
            row["cal_psf_mag"] = recurrenceList[0]["cal_psf_mag"]
            row["psf_inst_mag_sig"] = recurrenceList[0]["psf_inst_mag_sig"]
            row["filter"] = recurrenceList[0]["filter"]

            row["detection_list_id"] = QUBLISTS[row["detection_list_id"]]
            summaryw.writerow(row)

            # There will ALWAYS be at least one recurrence, so recurrenceList always present.
            recurrenceHeader = list(recurrenceList[0].keys())
            recurrencewh = csv.writer(recurrenceCSVFile, delimiter=delimiter)
            if writeRecurrenceHeaderFlag:
                recurrencewh.writerow(recurrenceHeader)
                writeRecurrenceHeaderFlag = False

            recurrencew = csv.DictWriter(recurrenceCSVFile, fieldnames=recurrenceHeader, delimiter=delimiter)

            for recRow in recurrenceList:
                recurrencew.writerow(recRow)



    summaryCSVFile.close()
    recurrenceCSVFile.close()

    return 0


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
        config = yaml.safe_load(yaml_file)

    username = config['databases']['local']['username']
    password = config['databases']['local']['password']
    database = config['databases']['local']['database']
    hostname = config['databases']['local']['hostname']

        
    conn = dbConnect(hostname, username, password, database)

    delimiter = options.delimiter
    detectionLists = [int(x) for x in options.lists.split(',')]

    # If the list isn't specified assume it's the Good List.
    if options.customlist is not None:
        if int(options.customlist) >= 1 and int(options.customlist) <= 100:
            customList = options.customlist
        else:
            print("The custom list must be between 1 and 100 inclusive.  Exiting.")
            sys.exit(1)

    conn = dbConnect(hostname, username, password, database)

    summaryCSVFilename = '/' + hostname + '/images/' + database + '/lightcurves/' + options.summaryfile
    recurrenceCSVFilename = '/' + hostname + '/images/' + database + '/lightcurves/' + options.recfile

    produceGenericCSV(conn, options, delimiter, detectionLists = detectionLists, summaryCSVFilename = summaryCSVFilename, recurrenceCSVFilename = recurrenceCSVFilename, queryType = EXCLUDE_AGNS)
    #producePESSTOCSV(conn, options, hostname, database, delimiter, customList, summaryCSVFilename = summaryCSVFilename, recurrenceCSVFilename = recurrenceCSVFilename)

    if options.writeAGNs:
        summaryCSVFilename = '/' + hostname + '/images/' + database + '/lightcurves/' + options.summaryfileagn
        recurrenceCSVFilename = '/' + hostname + '/images/' + database + '/lightcurves/' + options.recfileagn

        produceGenericCSV(conn, options, database, delimiter, detectionLists = [int(options.agnlist)], summaryCSVFilename = summaryCSVFilename, recurrenceCSVFilename = recurrenceCSVFilename, queryType = AGNS_ONLY)

    conn.close ()

    return


# ###########################################################################################
#                                         Main hook
# ###########################################################################################


if __name__=="__main__":
    main()
