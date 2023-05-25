#!/usr/bin/env python
"""Ingest ATLAS data files.

Usage:
  %s <configfile> <regex> [--ingester=<ingester>] [--days=<days>] [--maxjobs=<maxjobs>] [--camera=<camera>] [--mjd=<mjd>] [--verbose] [--atlasroot=<atlasroot>] [--pid=<pid>] [--difflocation=<difflocation>]
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
  --verbose                         Run verbosely.
  --atlasroot=<atlasroot>           ATLAS root location [default: /atlas/].
  --difflocation=<difflocation>     Diff catalogue location. E.g. /atlas/diff/CAMERA/fake/MJD.fake (caps = special variable). Null value means use standard ATLAS archive location.

E.g.:
  %s ~/config_fakers.yaml *.ddc --mjd=58940 --camera=01a --difflocation=/atlas/diff/CAMERA/fake/MJD.fake
"""

import sys
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt
import os, shutil, re, csv, subprocess
from gkutils.commonutils import Struct, cleanOptions
from gkutils.commonutils import find, dbConnect, getCurrentMJD
import optparse, warnings, subprocess, datetime
import glob
from collections import OrderedDict


# Script to Ingest ATLAS detecton files.
# We need to do the following:
# 1. Get the list of files to ingest (or pass the files on the argument list)
# 2. Sort the files into bins.  (Note that it would be useful to sort by boresight.)
# 3. Call the relevant ingester with the file sublist.


def getFilesIngested(conn, mjdthreshold, camera = '02a'):
    """
    Get all ingested files
    """

    import MySQLdb

    try:
        cursor = conn.cursor (MySQLdb.cursors.DictCursor)

        cursor.execute ("""
            select concat('/atlas/diff/',substr(expname,1,3),'/',truncate(mjd_obs,0),'/',expname,'.ddt') ddtfile
              from atlas_metadata
             where truncate(mjd_obs,0) > %s
               and substr(expname,1,3) = %s
          order by expname
        """,(mjdthreshold,camera))
        result_set = cursor.fetchall ()

        cursor.close ()

    except MySQLdb.Error as e:
        print("Error %d: %s" % (e.args[0], e.args[1]))
        sys.exit (1)

    fileList = []
    if result_set:
        fileList = [x['ddtfile'] for x in result_set]

    return fileList


def getFilesIngestedddc2(conn, mjdthreshold, mjdToIngest = None, camera = '02a'):
    """
    Get all ingested ddc files
    """

    import MySQLdb

    try:
        cursor = conn.cursor (MySQLdb.cursors.DictCursor)

        if mjdToIngest:
            cursor.execute ("""
                select concat(obs,'.ddc') ddcfile
                  from atlas_metadataddc
                 where truncate(mjd,0) > %s
                   and truncate(mjd,0) <= %s
                   and substr(obs,1,3) = %s
              order by obs
            """,(mjdthreshold, mjdToIngest, camera))
        else:
            cursor.execute ("""
                select concat(obs,'.ddc') ddcfile
                  from atlas_metadataddc
                 where truncate(mjd,0) > %s
                   and substr(obs,1,3) = %s
              order by obs
            """,(mjdthreshold,camera))
        result_set = cursor.fetchall ()

        cursor.close ()

    except MySQLdb.Error as e:
        print("Error %d: %s" % (e.args[0], e.args[1]))
        sys.exit (1)

    fileList = []
    if result_set:
        fileList = [x['ddcfile'] for x in result_set]

    return fileList




def getFilesIngestedddc(conn, mjdthreshold, mjdToIngest = None, camera = '02a'):
    """
    Get all ingested ddc files
    """

    import MySQLdb

    try:
        cursor = conn.cursor (MySQLdb.cursors.DictCursor)

        if mjdToIngest:
            cursor.execute ("""
                select concat('/atlas/diff/',substr(obs,1,3),'/',truncate(mjd,0),'/',obs,'.ddc') ddcfile
                  from atlas_metadataddc
                 where truncate(mjd,0) > %s
                   and truncate(mjd,0) <= %s
                   and substr(obs,1,3) = %s
              order by obs
            """,(mjdthreshold, mjdToIngest, camera))
        else:
            cursor.execute ("""
                select concat('/atlas/diff/',substr(obs,1,3),'/',truncate(mjd,0),'/',obs,'.ddc') ddcfile
                  from atlas_metadataddc
                 where truncate(mjd,0) > %s
                   and substr(obs,1,3) = %s
              order by obs
            """,(mjdthreshold,camera))
        result_set = cursor.fetchall ()

        cursor.close ()

    except MySQLdb.Error as e:
        print("Error %d: %s" % (e.args[0], e.args[1]))
        sys.exit (1)

    fileList = []
    if result_set:
        fileList = [x['ddcfile'] for x in result_set]

    return fileList

# 2019-01-30 KWS Added atlasbase as the default ingest root, but sometimes we just want
#                to ingest from a custom location. Hence allow this in future.
def getFiles(regex, camera, mjdToIngest = None, mjdthreshold = None, days = None, atlasroot='/atlas/', options = None):
    """getFiles.

    Args:
        regex:
        camera:
        mjdToIngest:
        mjdthreshold:
        days:
        atlasroot:
        options:
    """
    # If mjdToIngest is defined, ignore mjdThreshold. If neither
    # are defined, grab all the files.


    # Don't use find, use glob. It treats the whole argument as a regex.
    # e.g. directory = "/atlas/diff/" + camera "/5[0-9][0-9][0-9][0-9]", regex = *.ddc

    if mjdToIngest:
        if options is not None and options.difflocation is not None:
            directory = options.difflocation.replace('CAMERA', camera).replace('MJD', str(mjdToIngest))
        else:
            directory = atlasroot + "diff/" + camera + "/" + str(mjdToIngest)
        fileList = glob.glob(directory + '/' + regex)
    else:
        if mjdthreshold and days:
            fileList = []
            # 2023-01-06 KWS Get data for tomorrow (days + 1) as well, since STH tomorrow a day early.
            for day in range(days + 1):
                if options is not None and options.difflocation is not None:
                    directory = options.difflocation.replace('CAMERA', camera).replace('MJD', str(mjdthreshold + day))
                else:
                    directory = atlasroot + "diff/" + camera + "/%d" % (mjdthreshold + day)
                files = glob.glob(directory + '/' + regex)
                if files:
                    fileList += files
        else:
            if options is not None and options.difflocation is not None:
                directory = options.difflocation.replace('CAMERA', camera).replace('MJD', '/[56][0-9][0-9][0-9][0-9]')
            else:
                directory = atlasroot + "diff/" + camera + "/[56][0-9][0-9][0-9][0-9]"
            fileList = glob.glob(directory + '/' + regex)

    fileList.sort()

    return fileList


def ingesterWrapper(ingester, configFile, fileList):
    """ingesterWrapper.

    Args:
        ingester:
        configFile:
        fileList:
    """

    for fileToIngest in fileList:
        p = subprocess.Popen([ingester, configFile, fileToIngest], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output, errors = p.communicate()

        if output:
            print(str(output))

        if errors:
            print(str(errors))


def main():
    opts = docopt(__doc__, version='0.1')
    opts = cleanOptions(opts)
    options = Struct(**opts)

    configFile = options.configfile
    regex = options.regex

    import yaml
    with open(configFile) as yaml_file:
        config = yaml.load(yaml_file)

    username = config['databases']['local']['username']
    password = config['databases']['local']['password']
    database = config['databases']['local']['database']
    hostname = config['databases']['local']['hostname']

    conn = dbConnect(hostname, username, password, database)
    # 2023-03-13 KWS MySQLdb disables autocommit by default. Switch it on globally.
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
    mjdthreshold = int(todayMJD) - days + 1

    # Specified MJD trumps mjd Threshold, so just go as far back
    # as the specified date
    if mjdToIngest:
        mjdthreshold = int(mjdToIngest[0:5]) - 1

    ingester = options.ingester

    # Add tomorrow as well - for STH, CHL and TDO.
    days += 1

    fileList = getFiles(regex, camera, mjdToIngest = mjdToIngest, mjdthreshold = mjdthreshold, days = days, options = options)
    ingestedFiles = getFilesIngestedddc2(conn, mjdthreshold = mjdthreshold, mjdToIngest = mjdToIngest, camera = camera)

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

    ingesterWrapper(ingester, configFile, filesToIngest)

    conn.close ()
    return 0


if __name__=="__main__":
    main()
