#!/usr/bin/env python
"""Ingest ATLAS data files.

Usage:
  %s <configfile> <regex> [--ingester=<ingester>] [--days=<days>] [--maxjobs=<maxjobs>] [--camera=<camera>] [--mjd=<mjd>] [--verbose] [--atlasroot=<atlasroot>] [--pid=<pid>] [--loglocation=<loglocation>] [--logprefix=<logprefix>] [--difflocation=<difflocation>]
  %s (-h | --help)
  %s --version

Options:
  -h --help                         Show this screen.
  --version                         Show version.
  --ingester=<ingester>             C++ ingester executable [default: IngesterMainddc].
  --pid=<pid>                       Process ID [default: 1].
  --maxjobs=<maxjobs>               Maximum jobs to run [default: 100].
  --days=<days>                     Number of days to go back [default: 5].
  --camera=<camera>                 Camera, e.g. 01a, 02a [default: 02a].
  --mjd=<mjd>                       Modified Julian Day to ingest. Used to test ingest.
  --loglocation=<loglocation>       Log file location [default: /tmp/]
  --logprefix=<logprefix>           Log prefix [default: ingester]
  --verbose                         Run verbosely.
  --atlasroot=<atlasroot>           ATLAS root location [default: /atlas/].
  --difflocation=<difflocation>     Diff catalogue location. E.g. /atlas/diff/CAMERA/fake/MJD.fake (caps = special variable). Null value means use standard ATLAS archive location.

E.g.:
  %s ~/config_fakers.yaml *.ddc --mjd=58940 --camera=01a --difflocation=/atlas/diff/CAMERA/fake/MJD.fake
  %s ../../../../../atlas/config/config4_db5_readonly.yaml *.dnc --camera=01a --days=2 --loglocation=/db5/tc_logs/atlas4/ --ingester=/usr/local/ps1code/gitrelease/atlas/code/ingesters/tphot/cpp/IngesterMainddcnnc
"""

import sys
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt
import os, shutil, re, csv, subprocess

from ingesterWrapper import ingesterWrapper, getFilesIngested, getFiles, getFilesIngestedddc, getFilesIngestedddc2
from gkutils.commonutils import find, dbConnect, getCurrentMJD, splitList, parallelProcess
from gkutils.commonutils import Struct, cleanOptions
import warnings, subprocess, datetime
from collections import OrderedDict


# Script to Ingest ATLAS detecton files.
# We need to do the following:
# 1. Get the list of files to ingest (or pass the files on the argument list)
# 2. Sort the files into bins.  (Note that it would be useful to sort by boresight.)
# 3. Call the relevant ingester with the file sublist.


def worker(num, db, objectListFragment, dateAndTime, firstPass, miscParameters):
    """thread worker function"""
    # Redefine the output to be a log file.
    options = miscParameters[0]
    sys.stdout = open('%s%s_%s_%d.log' % (options.loglocation, options.logprefix, dateAndTime, num), "w")

    # This is in the worker function
    #ingester = options.ingester
    #configFile = options.configfile

    
    ingesterWrapper(options.ingester, options.configfile, objectListFragment)
    print("Process complete.")

    return 0


def main():
    opts = docopt(__doc__, version='0.1')
    opts = cleanOptions(opts)
    options = Struct(**opts)

    configFile = options.configfile
    regex = options.regex

    import yaml
    with open(configFile) as yaml_file:
        config = yaml.safe_load(yaml_file)

    username = config['databases']['local']['username']
    password = config['databases']['local']['password']
    database = config['databases']['local']['database']
    hostname = config['databases']['local']['hostname']

    db = []
    db.append(username)
    db.append(password)
    db.append(database)
    db.append(hostname)

    conn = dbConnect(hostname, username, password, database)
    # 2023-03-25 KWS MySQLdb disables autocommit by default. Switch it on globally.
    conn.autocommit(True)

    warnings.filterwarnings("ignore")

    # Parse command line

    currentDate = datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")
    (year, month, day, hour, min, sec) = currentDate.split(':')
    dateAndTime = "%s%s%s_%s%s%s" % (year, month, day, hour, min, sec)

    pid = int(options.pid)
    maxjobs = int(options.maxjobs)
    days = int(options.days)
    camera = options.camera
    try:
        mjdToIngest = options.mjd
    except TypeError as e:
        mjdToIngest = None



    print("camera =", camera)
    print("regex =", regex)

    todayMJD = getCurrentMJD()

    # Use + 1 to include today!
    mjdthreshold = int(todayMJD) - days + 1

    # Specified MJD trumps mjd Threshold, so just go as far back
    # as the specified date
    if mjdToIngest:
        mjdthreshold = int(mjdToIngest[0:5]) - 1

    ingester = options.ingester

    # Add tomorrow as well - for STH, CHL and TDO.
    days += 1

    fileList = getFiles(regex, camera, mjdToIngest = mjdToIngest, mjdthreshold = mjdthreshold, days = days, atlasroot=options.atlasroot, options = options)
    ingestedFiles = getFilesIngestedddc2(conn, mjdthreshold = mjdthreshold, camera = camera)



    fileListDict = OrderedDict()

    print("List of files...")
    for row in fileList:
        fileListDict[os.path.basename(row)] = row
        print(row)

    print("List of ingested files...")
    for row in ingestedFiles:
        print(row)

    filesToIngest = [fileListDict[x] for x in list(set(fileListDict.keys()) - set(ingestedFiles))]
    filesToIngest.sort()

    print("List of files to ingest...")
    for row in filesToIngest:
        print(row)


    print("TOTAL OBJECTS TO CHECK = %d" % len(filesToIngest))

    if len(fileList) > 0:
        # 2018-02-06 KWS Use half the default number of processes. This may ironically speed up ingest.
        nProcessors, listChunks = splitList(filesToIngest, bins = 32)

        print("%s Parallel Processing..." % (datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")))
        parallelProcess(db, dateAndTime, nProcessors, listChunks, worker, miscParameters = [options], drainQueues = False)
        print("%s Done Parallel Processing" % (datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")))

    conn.close ()
    return 0


if __name__=="__main__":
    main()
