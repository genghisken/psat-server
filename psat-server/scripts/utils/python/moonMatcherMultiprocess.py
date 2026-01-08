#!/usr/bin/env python3
"""Check for Solar System moons.

Usage:
  %s <configfile> [<candidate>...] [--list=<listid>] [--matchRadius=<radius>] [--matchTimeDelta=<timedelta>] [--update] [--date=<date>] [--ddc] [--loglocation=<loglocation>] [--logprefix=<logprefix>]
  %s (-h | --help)
  %s --version

Options:
  -h --help                     Show this screen.
  --version                     Show version.
  --list=<listid>               The object list [default: 4]
  --customlist=<customlistid>   The object custom list
  --matchRadius=<radius>        Match radius (arcsec) [default: 20]
  --matchTimeDelta=<timedelta>  Match time delta (days) [default: 0.5]
  --update                      Update the database
  --date=<date>                 Date threshold - no hyphens [default: 20130601]
  --survey=<survey>             Survey database to interrogate [default: atlas].
  --ddc                         Use the ddc schema (ignored if survey=panstarrs).
  --loglocation=<loglocation>   Log file location [default: /tmp/]
  --logprefix=<logprefix>       Log prefix [default: moons]

  Example:
    %s ../../../../config/config4_db4_readonly.yaml 1063629090302540900 --ddc --update
"""
import sys
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt
import os, MySQLdb, shutil, re
from gkutils.commonutils import find, Struct, cleanOptions, dbConnect, calculateRMSScatter, getAngularSeparation, coneSearchHTM, QUICK, FULL, CAT_ID_RA_DEC_COLS, PROCESSING_FLAGS, splitList, parallelProcess
import MySQLdb
sys.path.append('../../common/python')
from moonMatcher import moonMatcher, updateObjects
from queries import getATLASCandidates, getAtlasObjects, getPanSTARRSCandidates, updateTransientObservationAndProcessingStatus, insertTransientObjectComment, getObjectInfo


def worker(num, db, objectListFragment, dateAndTime, firstPass, miscParameters, q):
    """thread worker function"""
    # Redefine the output to be a log file.
    options = miscParameters[0]

    sys.stdout = open('%s%s_%s_%d.log' % (options.loglocation, options.logprefix, dateAndTime, num), "w")

    conn = dbConnect(db['hostname'], db['username'], db['password'], db['database'], quitOnError = True)

    # 2023-03-25 KWS MySQLdb disables autocommit by default. Switch it on globally.
    conn.autocommit(True)

    connCatalogues = dbConnect(db['cathost'], db['catuser'], db['catpass'], db['catname'], quitOnError = True)

    # This is in the worker function
    # 2015-02-17 KWS Note to self - I need to pass the radius through the miscParameters
    objectsForUpdate = moonMatcher(conn, connCatalogues, options, objectListFragment)

    # Write the objects for update onto a Queue object
    print("Adding %d objects onto the queue." % len(objectsForUpdate))

    q.put(objectsForUpdate)

    print("Process complete.")
    conn.close()
    print("DB Connection Closed - exiting")

    return 0


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

    candidateList = []

    detectionList = 4

    catuser = config['databases']['catalogues']['username']
    catpass = config['databases']['catalogues']['password']
    catname = config['databases']['catalogues']['database']
    cathost = config['databases']['catalogues']['hostname']

    db = {'username': username,
          'password': password,
          'database': database,
          'hostname': hostname,
          'catuser': catuser,
          'catpass': catpass,
          'catname': catname,
          'cathost': cathost}

    conn = dbConnect(hostname, username, password, database, quitOnError = True)

    # 2023-03-25 KWS MySQLdb disables autocommit by default. Switch it on globally.
    conn.autocommit(True)

    connCatalogues = dbConnect(cathost, catuser, catpass, catname, quitOnError = True)

    currentDate = datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")
    (year, month, day, hour, min, sec) = currentDate.split(':')
    dateAndTime = "%s%s%s_%s%s%s" % (year, month, day, hour, min, sec)


    # If the list isn't specified assume it's the Eyeball List.
    if options.list is not None:
        try:
            detectionList = int(options.list)
            if detectionList < 0 or detectionList > 8:
                print("Detection list must be between 0 and 8")
                return 1
        except ValueError as e:
            sys.exit("Detection list must be an integer")

    if options.date is not None:
        try:
            dateThreshold = '%s-%s-%s' % (options.date[0:4], options.date[4:6], options.date[6:8])
        except:
            dateThreshold = '2013-06-01'

    elif options.candidate or options.list or options.customList:
        # pull from DB into a list of dicts
        if options.survey == 'atlas':
            candidateList = getATLASCandidates(conn, options, processingFlags = PROCESSING_FLAGS['moons'])
        elif options.survey == 'panstarrs':
            candidateList = getPanSTARRSCandidates(conn, options, processingFlags = PROCESSING_FLAGS['moons'])

    # Multiprocessing code starts

    objectsForUpdate = []

    if len(candidateList) > 0:
        nProcessors, listChunks = splitList(candidateList)

        print("%s Parallel Processing..." % (datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")))
        objectsForUpdate = parallelProcess(db, dateAndTime, nProcessors, listChunks, worker, miscParameters = [options])
        print("%s Done Parallel Processing" % (datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")))

        print("TOTAL OBJECTS TO UPDATE = %d" % len(objectsForUpdate))

    if len(objectsForUpdate) > 0 and options.update:
        updateObjects(conn, options, objectsForUpdate)

    return





if __name__ == '__main__':
    main()
