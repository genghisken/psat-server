#!/usr/bin/env python3
"""Do a cone search for a catalogue of a csv file full of objectid, ra, dec. Return objectid, ra, dec, attributes

Usage:
  %s <configfile> <filename> <tablename> [<attributes>...] [--delimiter=<delimiter>]  [--matchradius=<matchradius>] [--outputfile=<outputFile>] [--nprocesses=<nprocesses>] [--loglocation=<loglocation>] [--logprefix=<logprefix>] [--namecolumn=<namecolumn>] [--racolumn=<racolumn>] [--deccolumn=<deccolumn>]
  %s (-h | --help)
  %s --version

Options:
  -h --help                      Show this screen.
  --version                      Show version.
  --delimiter=<delimiter>        Delimiter [default: ,]
  --matchradius=<matchradius>    Match radius [default: 2.0]
  --outputfile=<outputFile>      Output filename [default: /tmp/xmresults.csv]
  --nprocesses=<nprocesses>      Number of processes to use [default: 1].
  --loglocation=<loglocation>    Log file location [default: /tmp/]
  --logprefix=<logprefix>        Log prefix [default: coneSearch]
  --namecolumn=<namecolumn>      Column representing name [default: name]
  --racolumn=<racolumn>          Column representing RA [default: ra]
  --deccolumn=<deccolumn>        Column representing Declination [default: dec]

  Example:
    %s ../../../../config/config_readonly.yaml objects.csv tcs_cat_tns tns_name type
"""
# 2019-09-03 KWS Make python 3 compatible
import sys
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt
import os, MySQLdb, shutil, re
from gkutils.commonutils import find, Struct, cleanOptions, dbConnect, coneSearchHTM, QUICK, FULL, splitList, parallelProcess, readGenericDataFile
import MySQLdb
import numpy as np
import csv
import datetime

# ******** This code should be moved to a COMMON area ********

def crossmatchObjects(conn, options, objectList, matchRadius = 2.0, processNumber = None):

    objectsForUpdate = []

    for obj in objectList:
        message, results = coneSearchHTM(float(obj[options.racolumn]), float(obj[options.deccolumn]), float(options.matchradius), options.tablename, queryType = FULL, conn = conn)
        if results and len(results) >= 1:
            # We have more than one object.  No we need to merge anything. Can exit now.
            separation = results[0][0]
            obj['separation'] = separation
            for a in options.attributes:
                print (a)
                obj[a] = results[0][1][a]
            objectsForUpdate.append(obj)

    return objectsForUpdate


def worker(num, db, objectListFragment, dateAndTime, firstPass, miscParameters, q):
    """thread worker function"""
    # Redefine the output to be a log file.
    options = miscParameters[0]

    sys.stdout = open('%s%s_%s_%d.log' % (options.loglocation, options.logprefix, dateAndTime, num), "w")
    conn = None
    try:
        conn = dbConnect(db['hostname'], db['username'], db['password'], db['database'], quitOnError = True)
        conn.autocommit(True)
    except:
        print("Cannot connect to the local database. Terminating this process.")
        q.put([])
        return 0

    objectsForUpdate = crossmatchObjects(conn, options, objectListFragment, matchRadius = float(options.matchradius))

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
        config = yaml.load(yaml_file, Loader=yaml.SafeLoader)

    username = config['databases']['local']['username']
    password = config['databases']['local']['password']
    database = config['databases']['local']['database']
    hostname = config['databases']['local']['hostname']

    db = {}
    db['username']=username
    db['password']=password
    db['database']=database
    db['hostname']=hostname

    currentDate = datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")
    (year, month, day, hour, min, sec) = currentDate.split(':')
    dateAndTime = "%s%s%s_%s%s%s" % (year, month, day, hour, min, sec)

    data = readGenericDataFile(options.filename, delimiter='\t')

    if options.outputfile is not None:
        prefix = options.outputfile.split('.')[0]
        suffix = options.outputfile.split('.')[-1]
            
        if suffix == prefix:
            suffix = ''
        
        if suffix:
            suffix = '.' + suffix

    if len(data) == 1 or int(options.nprocesses) == 1:
        # Do it single threaded
        conn = dbConnect(hostname, username, password, database, quitOnError = True)

        objects = crossmatchObjects(conn, options, data, matchRadius = float(options.matchradius))

        if len(objects) > 0:
            with open('%s%s' % (prefix, suffix), 'w') as f:
                w = csv.DictWriter(f, objects[0].keys(), delimiter = ',')
                w.writeheader()
                for row in objects:
                    w.writerow(row)


        if conn:
            conn.close()

    elif len(data) > 1:
        # Use multiprocessing
        print("%s Parallel Processing..." % (datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")))
        nProcessors, listChunks = splitList(data, bins=int(options.nprocesses))
        objects = parallelProcess(db, dateAndTime, nProcessors, listChunks, worker, miscParameters = [options], firstPass = True)
        print("%s Done Parallel Processing" % (datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")))

        if len(objects) > 0:
            with open('%s%s' % (prefix, suffix), 'w') as f:
                w = csv.DictWriter(f, objects[0].keys(), delimiter = ',')
                w.writeheader()
                for row in objects:
                    w.writerow(row)



    return



if __name__ == '__main__':
    main()
