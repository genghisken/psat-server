#!/usr/bin/env python
"""How many detections are there in each diff exposure subcell.

Usage:
  %s <configfile> [<exposure>...] [--regex=<regex>] [--days=<n>] [--camera=<camera>] [--mjd=<mjd>]
  %s (-h | --help)
  %s --version

Options:
  -h --help                    Show this screen.
  --version                    Show version.
  --regex=<regex>              The regex. [default: *.difpar].
  --days=<n>                   Number of days from today [default: 5].
  --mjd=<mjd>                  Just the specified MJD.
  --camera=<camera>            Camera [default: 02a].

"""
import sys
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt
import os, MySQLdb, shutil, re
from gkutils.commonutils import find, Struct, cleanOptions, getCurrentMJD, dbConnect, readGenericDataFile, nullValue, intValue, floatValue
import glob

# Each log contains 64 lines for each of the 8x8 diff subcells. An example show below.

#Cnv   Region                 Nstamp trim sumkern sigstmp Avscat  sigstmp  Fscat  GOODrat  OKrat  x2norm diffrat
#Tmp   0 [0:1319,0:1319]           9   0   2.060   6.171   4.626   6.759   5.027   1.379   2.177 203.198   1.579
#Tmp   1 [1319:2639,0:1319]        9   0   2.112   3.382   1.713   3.518   1.813   1.254   2.025 231.706   1.614
#Tmp   2 [2639:3959,0:1319]        9   0   2.111   3.874   1.545   3.901   1.561   1.432   2.184 205.968   1.525

# It's easily readable with readGenericDataFile, but I need to add an extra column (pixelrange). In this case, I
# can use something like - noting that the sigstmp column appears twice:

#Cnv   Region pixelrange      Nstamp trim sumkern sigstmp1 Avscat  sigstmp2  Fscat  GOODrat  OKrat  x2norm diffrat

# and then read the data from row 1 onwards. Row 0 will be the header.

def getFiles(regex, camera, mjdToIngest = None, mjdthreshold = None, days = None):
    # If mjdToIngest is defined, ignore mjdThreshold. If neither
    # are defined, grab all the files.


    # Don't use find, use glob. It treats the whole argument as a regex.
    # e.g. directory = "/atlas/diff/" + camera "/5[0-9][0-9][0-9][0-9]/AUX", regex = *.difpar

    if mjdToIngest:
        directory = "/atlas/diff/" + camera + "/" + str(mjdToIngest) + '/AUX'
        fileList = glob.glob(directory + '/' + regex)
    else:
        if mjdthreshold and days:
            fileList = []
            for day in range(days):
                directory = "/atlas/diff/" + camera + "/%d" % (mjdthreshold + day) + '/AUX'
                files = glob.glob(directory + '/' + regex)
                if files:
                    fileList += files
        else:
            # 2023-02-24 KWS Found in the nick of time! Added [56] to regex.
            directory = "/atlas/diff/" + camera + "/[56][0-9][0-9][0-9][0-9]/AUX"
            fileList = glob.glob(directory + '/' + regex)

    fileList.sort()

    return fileList


def getExposuresPerMJD(conn, mjd, ddc = True):
   """
   Get all exposures on a specified MJD
   """

   try:
      cursor = conn.cursor (MySQLdb.cursors.DictCursor)

      if ddc:
          cursor.execute ("""
                select obs
                  from atlas_metadataddc
                 where truncate(mjd, 0) = %s
          """, (mjd,))
      else:
          cursor.execute ("""
                select expname
                  from atlas_metadata
                 where truncate(mjd, 0) = %s
          """, (mjd,))
      resultSet = cursor.fetchall ()

      cursor.close ()

   except MySQLdb.Error as e:
      print(str(e))
      sys.exit (1)


   return resultSet


def getATLASDiffLogFilesIngested(conn, mjdthreshold = None, mjdToIngest = None, camera = '02a', exposure = None):
    """
    Get all ingested ddc files
    """

    import MySQLdb

    try:
        cursor = conn.cursor (MySQLdb.cursors.DictCursor)

        if exposure:
            cursor.execute ("""
                select concat('/atlas/diff/',substr(obs,1,3),'/',substr(obs,4,5),'/AUX/',obs,'.difpar') difflog
                  from atlas_diff_subcell_logs
                 where obs = %s
            """,(exposure,))
        elif mjdToIngest:
            cursor.execute ("""
                select concat('/atlas/diff/',substr(obs,1,3),'/',substr(obs,4,5),'/AUX/',obs,'.difpar') difflog
                  from atlas_diff_subcell_logs
                 where obs > %s
                   and obs < %s
              order by obs
            """,(camera + str(mjdToIngest), camera + str(mjdthreshold)))
        else:
            print(mjdToIngest, mjdthreshold)
            cursor.execute ("""
                select concat('/atlas/diff/',substr(obs,1,3),'/',substr(obs,4,5),'/AUX/',obs,'.difpar') difflog
                  from atlas_diff_subcell_logs
                 where obs like concat (%s, '%%')
                   and obs > %s
              order by obs
            """,(camera, camera + str(mjdthreshold),))
        result_set = cursor.fetchall ()

        cursor.close ()

    except MySQLdb.Error as e:
        print(str(e))
        sys.exit (1)

    fileList = []
    if result_set:
        fileList = [x['difflog'] for x in result_set]

    return fileList



def readLog(filename):
    data = readGenericDataFile(filename, fieldnames = "cnv region pixelrange nstamp trim sumkern sigstmp1 avscat sigstmp2 fscat goodrat okrat x2norm diffrat".split(), skipLines = 1)
    obs = filename.split('/')[-1].split('.')[0]
    for row in data:
        # Add separate columns for x1, y1, x2, y2
        range1, range2 = row['pixelrange'].replace('[','').replace(']','').split(',')
        x1, x2 = range1.split(':')
        y1, y2 = range2.split(':')
        row['obs'] = obs
        row['x1'] = x1
        row['x2'] = x2
        row['y1'] = y1
        row['y2'] = y2
    return data

def insertATLASLogRow(conn, row):

    try:
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)

        cursor.execute ("""
          insert into atlas_diff_subcell_logs (
                                            obs,
                                            cnv,
                                            region,
                                            x1,
                                            x2,
                                            y1,
                                            y2,
                                            nstamp,
                                            trim,
                                            sumkern,
                                            sigstmp1,
                                            avscat,
                                            sigstmp2,
                                            fscat,
                                            goodrat,
                                            okrat,
                                            x2norm,
                                            diffrat
                                            )
          values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
          """, (
                row['obs'],
                row['cnv'],
                intValue(row['region']),
                intValue(row['x1']),
                intValue(row['x2']),
                intValue(row['y1']),
                intValue(row['y2']),
                intValue(row['nstamp']),
                intValue(row['trim']),
                floatValue(row['sumkern']),
                floatValue(row['sigstmp1']),
                floatValue(row['avscat']),
                floatValue(row['sigstmp2']),
                floatValue(row['fscat']),
                floatValue(row['goodrat']),
                floatValue(row['okrat']),
                floatValue(row['x2norm']),
                floatValue(row['diffrat'])
                ))
        cursor.close()

    except MySQLdb.Error as e:
        print(str(e))
        pass

    cursor.close()
    return conn.insert_id()

def ingestLogs(mjds):
    return



def populateDiffLogs(options):
    import yaml
    with open(options.configfile) as yaml_file:
        config = yaml.load(yaml_file)

    username = config['databases']['local']['username']
    password = config['databases']['local']['password']
    database = config['databases']['local']['database']
    hostname = config['databases']['local']['hostname']

    conn = dbConnect(hostname, username, password, database)
    if not conn:
        print("Cannot connect to the database")
        return 1

    # 2023-03-25 KWS MySQLdb disables autocommit by default. Switch it on globally.
    conn.autocommit(True)
    todayMJD = getCurrentMJD()

    # Use + 1 to include today!
    mjdthreshold = int(todayMJD) - int(options.days) + 1

    # Specified MJD trumps mjd Threshold, so just go as far back
    # as the specified date
    mjdToIngest = None
    if options.mjd:
        mjdthreshold = int(options.mjd) - 1
        mjdToIngest = int(options.mjd)

    # Specified exposure list trumps all.
    if options.exposure:
        fileList = ['/atlas/diff/%s/%s/AUX/%s.difpar' % (x[0:3],x[3:8],x) for x in options.exposure]
        ingestedFiles = []
        for exp in options.exposure:
            e = getATLASDiffLogFilesIngested(conn, exposure = exp)
            ingestedFiles += e
    else:
        print("Getting file list...")
        fileList = getFiles(options.regex, options.camera, mjdToIngest = mjdToIngest, mjdthreshold = mjdthreshold, days = int(options.days))
        print("Getting ingested file list...")
        ingestedFiles = getATLASDiffLogFilesIngested(conn, mjdthreshold = mjdthreshold, camera = options.camera)

    ingestedFiles = list(set(ingestedFiles))
    print(len(ingestedFiles))

    print("List of files...")
    for row in fileList:
        print(row)

    print("List of ingested files...")
    for row in ingestedFiles:
        print(row)

    filesToIngest = list(set(fileList) - set(ingestedFiles))
    filesToIngest.sort()

    print("List of files to ingest...")
    for row in filesToIngest:
        print(row)


    for row in filesToIngest:
        data = readLog(row)
        for d in data:
            insertATLASLogRow(conn, d)
            for k,v in d.items():
                print("%s = %s" % (k,v))
            print()




    conn.close ()


def main(argv = None):
    opts = docopt(__doc__, version='0.1')
    opts = cleanOptions(opts)

    # Use utils.Struct to convert the dict into an object for compatibility with old optparse code.
    options = Struct(**opts)
    populateDiffLogs(options)

if __name__ == '__main__':
    main()
