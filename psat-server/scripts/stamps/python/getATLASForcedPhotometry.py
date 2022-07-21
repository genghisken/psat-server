#!/usr/bin/env python
"""Do ATLAS forced photometry.

Usage:
  %s <configfile> [<candidate>...] [--detectionlist=<detectionlist>] [--customlist=<customlist>] [--limit=<limit>] [--limitafter=<limitafter>] [--update] [--ddc] [--skipdownload] [--redregex=<redregex>] [--diffregex=<diffregex>] [--redlocation=<redlocation>] [--difflocation=<difflocation>] [--tphorce=<tphorcelocation>] [--useflagdate] [--test]
  %s (-h | --help)
  %s --version

Options:
  -h --help                                   Show this screen.
  --version                                   Show version.
  --update                                    Update the database
  --detectionlist=<detectionlist>             List option
  --customlist=<customlist>                   Custom List option
  --limit=<limit>                             Number of days before first detection (or flag date) to check [default: 0]
  --limitafter=<limitafter>                   Number of days after first detection (or flag date) to check [default: 150]
  --ddc                                       Use the DDC schema for queries
  --skipdownload                              Do not attempt to download the exposures (assumes they already exist locally)
  --redregex=<redregex>                       Reduced image regular expression. Caps = variable. [default: EXPNAME.fits.fz]
  --diffregex=<diffregex>                     Diff image regular expression. Caps = variable. [default: EXPNAME.diff.fz]
  --redlocation=<redlocation>                 Reduced image location. E.g. /atlas/diff/CAMERA/fake/MJD.fake (caps = special variable).  Null value means use standard ATLAS archive location.
  --difflocation=<difflocation>               Diff image location. E.g. /atlas/diff/CAMERA/fake/MJD.fake (caps = special variable). Null value means use standard ATLAS archive location.
  --tphorce=<tphorcelocation>                 Location of the tphorce shell script [default: /usr/local/ps1code/gitrelease/tphorce/tphorce].
  --useflagdate                               Use the flag date as the threshold for the number of days instead of the first detection (which might be rogue).
  --test                                      Just list the exposures for which we will do forced photometry.


E.g.:
  %s ../../../../config/config4_db4.yaml 1105142531182852400 --limit 30 --update --ddc --useflagdate
"""

import sys
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt
import os, shutil, re, csv, subprocess, io


from psat_server_web.atlas.atlas.commonqueries import getLightcurvePoints, getNonDetections, getNonDetectionsUsingATLASFootprint, ATLAS_METADATADDC, filterWhereClauseddc, LC_POINTS_QUERY_ATLAS_DDC, FILTERS
from makeATLASStamps import doRsync, getObjectsByList, getObjectsByCustomList
from gkutils.commonutils import dbConnect, PROCESSING_FLAGS, calculateRMSScatter, Struct, cleanOptions, readGenericDataFile, getMJDFromSqlDate

ATLAS_ROOT = '/atlas'
FORCED_PHOT_DET_SNR = 1.0
FORCED_PHOT_LIMIT_SNR = 3.0

#FORCED_PHOT_DET_SNR = 3.0
#FORCED_PHOT_LIMIT_SNR = 3.0

def getATLASObject(conn, objectId):
    """getATLASObject.

    Args:
        conn:
        objectId:
    """
    import MySQLdb

    try:
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute ("""
            select id, followup_id, ra, `dec`, atlas_designation 'name', object_classification, followup_flag_date
            from atlas_diff_objects
            where id = %s
        """, (objectId,))
        resultSet = cursor.fetchone ()

        cursor.close ()

    except MySQLdb.Error as e:
        print("Error %d: %s" % (e.args[0], e.args[1]))
        sys.exit (1)

    return resultSet


def getForcedPhotometryUniqueExposures(conn, candidateList, discoveryLimit = 20, cutoffLimit = 100, incremental = True, ddc = False, useFlagDate = False):
    """getForcedPhotometryUniqueExposures.

    Args:
        conn:
        candidateList:
        discoveryLimit:
        incremental:
        ddc:
        useFlagDate:
    """


    # The algorithm is as follows:
    # Download all the unique exposures required (multiprocessing)
    # Download all the unique tphot files required (multiprocessing)
    # For each object:
    #    For each exposure:
    #        grab the forced photometry info from tphorce.

    exposures = []
    perObjectExposures = {}
    for candidate in candidateList:
        existingExposures = []
        if incremental:
            # get existing forced photometry
            fp = getAtlasForcedPhotometry(conn, candidate['id'])
            existingExposures = [e['expname'] for e in fp]

        objectExps = []
        objectCoords = []
        if ddc:
            p, recurrences = getLightcurvePoints(candidate['id'], lcQuery=LC_POINTS_QUERY_ATLAS_DDC + filterWhereClauseddc(FILTERS), conn = conn)
        else:
            p, recurrences = getLightcurvePoints(candidate['id'], conn = conn)
        # 2017-08-31 KWS Introduced the new ATLAS Footprint based non-detection code.
        if ddc:
            b, blanks, lastNonDetection = getNonDetectionsUsingATLASFootprint(recurrences, conn = conn, ndQuery=ATLAS_METADATADDC, filterWhereClause = filterWhereClauseddc, catalogueName = 'atlas_metadataddc')
        else:
            b, blanks, lastNonDetection = getNonDetectionsUsingATLASFootprint(recurrences, conn = conn)

        firstDetection = recurrences[0].mjd
        if useFlagDate:
            firstDetection = getMJDFromSqlDate(candidate['followup_flag_date'].strftime("%Y-%m-%d") + ' 00:00:00') 

        print("First Detection MJD = %d" % firstDetection)
        print("Earliest Limit = %d" % (firstDetection - discoveryLimit) )
        print("Cutoff Limit = %d" % (firstDetection + cutoffLimit) )
        for row in recurrences:
            if row.mjd >= firstDetection - discoveryLimit and row.mjd <= firstDetection + cutoffLimit:
                objectCoords.append({'RA': row.ra, 'DEC': row.dec})
                if row.expname not in existingExposures:
                    objectExps.append(row.expname)
                    exposures.append(row.expname)

        avgRa = None
        avgDec = None
        rms = None
        if objectCoords:
            avgRa, avgDec, rms = calculateRMSScatter(objectCoords)

        for row in blanks:
            if row.mjd >= firstDetection - discoveryLimit and row.mjd <= firstDetection + cutoffLimit:
                if row.expname not in existingExposures:
                    objectExps.append(row.expname)
                    exposures.append(row.expname)

        perObjectExposures[candidate['id']] = {'exps': sorted(list(set(objectExps))), 'avgRa': avgRa, 'avgDec': avgDec}

    uniqueExposures = sorted(list(set(exposures)))
    return perObjectExposures, uniqueExposures

def doForcedPhotometry(options, objectList, perObjectExps):
    """doForcedPhotometry.

    Args:
        objectList:
        perObjectExps:
    """
    fphot = []
    for candidate in objectList:
        #print candidate['id'], perObjectExps[candidate['id']]['avgRa'], perObjectExps[candidate['id']]['avgDec']

        print("Running forced photometry for candidate", candidate['id'])

        #tphorce [options] <diffPath> <tphotOutputPath> <raDeg> <decDeg> <snrLimit>
        for exp in perObjectExps[candidate['id']]['exps']:
            camera = exp[0:3]
            mjd = exp[3:8]

            aux = ''
            imageName = ATLAS_ROOT + '/diff/' + camera + '/' + mjd + '/' + exp + '.diff.fz'

#            if camera == '02a' and int(mjd) <= 57771:
#                camera = '02a.ORIG'

            if int(mjd) >= 57350:
                aux = 'AUX/'
            tphName = ATLAS_ROOT + '/red/' + camera + '/' + mjd + '/' + aux + exp + '.tph'

            #print TPHORCE, imageName, tphName, perObjectExps[candidate['id']]['avgRa'], perObjectExps[candidate['id']]['avgDec'], '3.0'
            # 2022-07-21 KWS Added text=True to the Popen command. Ensures that the response comes back as text.
            p = subprocess.Popen([options.tphorce, imageName, tphName, str(perObjectExps[candidate['id']]['avgRa']), str(perObjectExps[candidate['id']]['avgDec']), str(FORCED_PHOT_DET_SNR), str(FORCED_PHOT_LIMIT_SNR)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            output, errors = p.communicate()
    
            if output.strip():
                print(output)
                csvData = readGenericDataFile(io.StringIO(output), delimiter = ',')
                # There should only be one CSV row
                data = None
                try:
                    data = csvData[0]
                except IndexError as e:
                    print("ERROR: This is not a CSV file. Output = %s" % str(output).strip())

                if data:
                    data['limiting_mag'] = False

# {'mjd': '57604.56804193', 'magerr': 'NaN', 'filter': 'c', 'expname': '02a57604o0375c', 'snr': 'NaN', 'mag': 'NaN', 'zp': '22.160', 'id': 1012602110150218500L}

                    if data['mag'] and '>' in data['mag']:
                        data['mag'] = data['mag'].replace('>','')
                        data['limiting_mag'] = True

                    data['id'] = candidate['id']
                    data['expname'] = exp
                    data['ra'] = perObjectExps[candidate['id']]['avgRa']
                    data['dec'] = perObjectExps[candidate['id']]['avgDec']
                    data['snrdet'] = FORCED_PHOT_DET_SNR
                    data['snrlimit'] = FORCED_PHOT_LIMIT_SNR

                    # Clean up the data - replace 'NaN' with None
                    for k,v in data.items():
                        if v == 'NaN':
                            data[k] = None
                    fphot.append(data)

            if errors.strip():
                print(errors)

    return fphot

# 2017-08-07 KWS Added new columns to the forced photometry

def getAtlasForcedPhotometry(conn, objectId):
    """
    Get existing forced photometry for a given object
    """

    import MySQLdb
    try:
        cursor = conn.cursor (MySQLdb.cursors.DictCursor)

        cursor.execute ("""
              SELECT expname,
                     mjd_obs,
                     filter,
                     ra,
                     `dec`,
                     mag,
                     dm,
                     snr,
                     zp,
                     x,
                     y,
                     peakval,
                     skyval,
                     peakfit,
                     dpeak,
                     skyfit,
                     flux,
                     dflux,
                     chin,
                     major,
                     minor,
                     snrdet,
                     snrlimit,
                     apfit,
                     date_inserted,
                     limiting_mag,
                     redo_photometry
              FROM atlas_forced_photometry
              where atlas_object_id=%s
              ORDER by mjd_obs
        """, (objectId,))
        resultSet = cursor.fetchall ()

        cursor.close ()

    except MySQLdb.Error as e:
        print("Error %d: %s" % (e.args[0], e.args[1]))
        sys.exit (1)


    return resultSet

def insertForcedPhotometryRow(conn, row):
    """insertForcedPhotometryRow.

    Args:
        conn:
        row:
    """

    import MySQLdb
    try:
        cursor = conn.cursor (MySQLdb.cursors.DictCursor)

        cursor.execute ("""
             insert into atlas_forced_photometry (atlas_object_id,
                                                  expname,
                                                  mjd_obs,
                                                  ra,
                                                  `dec`,
                                                  filter,
                                                  mag,
                                                  dm,
                                                  snr,
                                                  zp,
                                                  x,
                                                  y,
                                                  peakval,
                                                  skyval,
                                                  peakfit,
                                                  dpeak,
                                                  skyfit,
                                                  flux,
                                                  dflux,
                                                  chin,
                                                  major,
                                                  minor,
                                                  snrdet,
                                                  snrlimit,
                                                  apfit,
                                                  date_inserted,
                                                  limiting_mag,
                                                  redo_photometry)
             values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), %s, %s)
             """, (row['id'],
                   row['expname'],
                   row['mjd'],
                   row['ra'],
                   row['dec'],
                   row['filter'],
                   row['mag'],
                   row['magerr'],
                   row['snr'],
                   row['zp'],
                   row['x'],
                   row['y'],
                   row['peakval'],
                   row['skyval'],
                   row['peakfit'],
                   row['dpeak'],
                   row['skyfit'],
                   row['flux'],
                   row['dflux'],
                   row['chi/N'],
                   row['major'],
                   row['minor'],
                   row['snrdet'],
                   row['snrlimit'],
                   row['apfit'],
                   row['limiting_mag'],
                   False))

    except MySQLdb.Error as e:
        sys.stderr.write("Error %d: %s\n" % (e.args[0], e.args[1]))

    cursor.close ()
    return conn.insert_id()


def insertForcedPhotometry(conn, forcedPhotometry):
    """insertForcedPhotometry.

    Args:
        conn:
        forcedPhotometry:
    """
    for row in forcedPhotometry:
        print(row)
        rowId = insertForcedPhotometryRow(conn, row)


# 2016-09-07 KWS Created this to allow exposures to be downloaded via multiprocessing
def downloadFPExposures(exposureSet):
    """downloadFPExposures.

    Args:
        exposureSet:
    """
    doRsync(exposureSet, 'diff')
    # Grab the tphot photometry files
    doRsync(exposureSet, 'red', getMetadata = True, metadataExtension = '.tph')
    return exposureSet


def main(argv = None):

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

    update = options.update
    limit = int(options.limit)
    limitafter = int(options.limitafter)


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

    update = options.update
    limit = int(options.limit)

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


    conn = dbConnect(hostname, username, password, database)

    perObjectExps, allExps = getForcedPhotometryUniqueExposures(conn, objectList, discoveryLimit = limit, cutoffLimit = limitafter, incremental = True, ddc = options.ddc, useFlagDate = options.useflagdate)
    if options.test:
        for obj in objectList:
            print(obj['id'])
            for exp in perObjectExps[obj['id']]['exps']:
                print(exp)
            
        return 0

    if not options.skipdownload:
        doRsync(allExps, 'diff')
        doRsync(allExps, 'red', getMetadata = True, metadataExtension = '.tph')

    fphot = doForcedPhotometry(options, objectList, perObjectExps)

    insertForcedPhotometry(conn, fphot)

if __name__ == '__main__':
    main()
