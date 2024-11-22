#!/usr/bin/env python
"""Refactored External Crossmatch code for ATLAS.

Usage:
  %s <configfile> [<candidate>...] [--list=<listid>] [--customlist=<customlistid>] [--searchRadius=<radius>] [--update] [--date=<date>] [--ddc] [--updateSNType] [--numberOfThreads=<n>] [--tnsOnly] [--loglocation=<loglocation>] [--logprefix=<logprefix>]
  %s (-h | --help)
  %s --version

Options:
  -h --help                     Show this screen.
  --version                     Show version.
  --list=<listid>               The object list [default: 4]
  --customlist=<customlistid>   The object custom list
  --searchRadius=<radius>       Match radius (arcsec) [default: 3]
  --update                      Update the database
  --updateSNType                Update the Supernova Type in the objects table.
  --date=<date>                 Date threshold - no hyphens. If date is a small number assume number of days before NOW [default: 20160601]
  --numberOfThreads=<n>         Number of threads (stops external database overloading) [default: 10]
  --ddc                         Use the ddc schema
  --tnsOnly                     Only search the TNS database.
  --loglocation=<loglocation>   Log file location [default: /tmp/]
  --logprefix=<logprefix>       Log prefix [default: external_crossmatches]

  Example:
    %s ../../../../../atlas/config/config4_db5_readonly.yaml 1063629090302540900 --ddc --update --updateSNType --numberOfThreads=8
    %s ../../../../../atlas/config/config4_db5.yaml --list=0 --date=20240901 --ddc --update --updateSNType --numberOfThreads=32 --tnsOnly
"""
import sys
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt

import os, MySQLdb, shutil, re
from gkutils.commonutils import find, Struct, cleanOptions, dbConnect, calculateRMSScatter, getAngularSeparation, coneSearchHTM, QUICK, FULL, CAT_ID_RA_DEC_COLS, PROCESSING_FLAGS, splitList, parallelProcess

from externalTransientCrossmatchATLAS import getAtlasObjects, getAtlasObjectsByCustomList, getCatalogueTables, deleteExternalCrossmatches, insertExternalCrossmatches, updateObjectSpecType, crossmatchExternalLists
import datetime


def worker(num, db, objectListFragment, dateAndTime, firstPass, miscParameters, q):
    """thread worker function"""
    # Redefine the output to be a log file.
    options = miscParameters[0]
    sys.stdout = open('%s%s_%s_%d.log' % (options.loglocation, options.logprefix, dateAndTime, num), "w")
    conn = None
    try:
        conn = dbConnect(db['hostname'], db['username'], db['password'], db['database'], quitOnError = True)

        # 2023-03-25 KWS MySQLdb disables autocommit by default. Switch it on globally.
        conn.autocommit(True)
    except:
        print("Cannot connect to the local database. Terminating this process.")
        q.put([])
        return 0

    connPESSTO = None
    if not options.tnsOnly:
        try:
            connPESSTO = dbConnect(db['pesstohost'], db['pesstouser'], db['pesstopass'], db['pesstoname'], lport = db['pesstoport'], quitOnError = True)
        except:
            print("Cannot connect to the PESSTO database. Terminating this process.")
            q.put([])
            return 0

    connCatalogues = None
    try:
        connCatalogues = dbConnect(db['cathost'], db['catuser'], db['catpass'], db['catname'], quitOnError = True)
    except:
        print("Cannot connect to the catalogues database. Terminating this process.")
        q.put([])
        return 0

    # This is in the worker function
    # 2015-02-17 KWS Note to self - I need to pass the radius through the miscParameters
    tables = miscParameters[1]
    searchRadius = miscParameters[2]

    objectsForUpdate = crossmatchExternalLists(conn, connPESSTO, connCatalogues, objectListFragment, tables, searchRadius = searchRadius, tnsOnly = options.tnsOnly)

    # Write the objects for update onto a Queue object
    print("Adding %d objects onto the queue." % len(objectsForUpdate))

    q.put(objectsForUpdate)

    print("Process complete.")
    conn.close()
    if not options.tnsOnly:
        connPESSTO.close()
    connCatalogues.close()
    print("DB Connection Closed - exiting")

    return 0

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

    catuser = config['databases']['catalogues']['username']
    catpass = config['databases']['catalogues']['password']
    catname = config['databases']['catalogues']['database']
    cathost = config['databases']['catalogues']['hostname']

    PESSTOCATUSER = config['databases']['pessto']['username']
    PESSTOCATPASS = config['databases']['pessto']['password']
    PESSTOCATNAME = config['databases']['pessto']['database']
    PESSTOCATHOST = config['databases']['pessto']['hostname']

    PESSTOCATPORT = 3306
    try:
        PESSTOCATPORT = config['databases']['pessto']['port']
    except KeyError as e:
        pass

    db = {'username': username,
          'password': password,
          'database': database,
          'hostname': hostname,
          'pesstouser': PESSTOCATUSER,
          'pesstopass': PESSTOCATPASS,
          'pesstoname': PESSTOCATNAME,
          'pesstohost': PESSTOCATHOST,
          'pesstoport': PESSTOCATPORT,
          'catuser': catuser,
          'catpass': catpass,
          'catname': catname,
          'cathost': cathost}


    conn = dbConnect(hostname, username, password, database, quitOnError = True)

    # 2023-03-25 KWS MySQLdb disables autocommit by default. Switch it on globally.
    conn.autocommit(True)

    currentDate = datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")
    (year, month, day, hour, min, sec) = currentDate.split(':')
    dateAndTime = "%s%s%s_%s%s%s" % (year, month, day, hour, min, sec)


    if options.list is not None:
        try:
            detectionList = int(options.list)
            if detectionList < 0 or detectionList > 8:
                print("Detection list must be between 0 and 8")
                return 1
        except ValueError as e:
            sys.exit("Detection list must be an integer")

    if options.customlist is not None:
        try:
            customlist = int(options.customlist)
            if customlist < 0 or customlist > 100:
                print("Custom list must be between 0 and 100")
                return 1
        except ValueError as e:
            sys.exit("Custom list must be an integer")


    if options.date is not None:
        try:
            if len(options.date) == 8:
                dateThreshold = '%s-%s-%s' % (options.date[0:4], options.date[4:6], options.date[6:8])
            else:
                # Assume the date value is a number. Must be less than 60 for the time being.
                days = 30
                try:
                    days = int(options.date)
                except ValueError as e:
                    days = 30
                if days > 60:
                    days = 60
                dateThreshold = (datetime.datetime.now() - datetime.timedelta(days=days)).strftime("%Y-%m-%d")
        except:
            dateThreshold = '2016-06-01'

    if len(options.candidate) > 0:
        for row in options.candidate:
            object = getAtlasObjects(conn, objectId = int(row))
            if object:
                candidateList.append(object)

    elif options.customlist is not None:
        candidateList = getAtlasObjectsByCustomList(conn, listId = int(options.customlist))

    else:
        # Get only the ATLAS objects that don't have the 'moons' flag set.
        candidateList = getAtlasObjects(conn, listId = detectionList, dateThreshold = dateThreshold)

    tables = getCatalogueTables(conn)

    objectsForUpdate = []

    if len(candidateList) > 0:
        nProcessors, listChunks = splitList(candidateList, bins = int(options.numberOfThreads))

        print("%s Parallel Processing..." % (datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")))
        objectsForUpdate = parallelProcess(db, dateAndTime, nProcessors, listChunks, worker, miscParameters = [options, tables, float(options.searchRadius)])
        print("%s Done Parallel Processing" % (datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")))

        print("TOTAL OBJECTS TO UPDATE = %d" % len(objectsForUpdate))

    if options.update and len(objectsForUpdate) > 0:
        for result in objectsForUpdate:
            deleteExternalCrossmatches(conn, result['id'])
            for matchrow in result['matches']:
                if not ((matchrow['matched_list'] == 'The PESSTO Transient Objects - primary object') and matchrow['ps1_designation'] == matchrow['external_designation']):
                    # Don't bother ingesting ourself from the PESSTO Marshall
                    print(matchrow['matched_list'], matchrow['ps1_designation'],  matchrow['external_designation'])
                    insertId = insertExternalCrossmatches(conn, matchrow)
                if options.updateSNType and matchrow['matched_list'] == 'Transient Name Server':
                    # Update the observation_status (spectral type) of the object according to
                    # what is in the TNS.
                    updateObjectSpecType(conn, result['id'], matchrow['type'])

    conn.close ()


    return





if __name__ == '__main__':
    main()
