#!/usr/bin/env python
"""Make PS1 Finders for ATLAS objects above -30 degrees declination.

Usage:
  %s <configfile> [<candidate>...] [--detectionlist=<detectionlist>] [--customlist=<customlist>] [--update] [--ddc] [--size=<size>] [--flagdate=<flagdate>] [--filters=<filters>] [--singlefilter=<singlefilter>] [--downloadpath=<downloadpath>] [--nsigma=<nsigma>]
  %s (-h | --help)
  %s --version

Options:
  -h --help                                   Show this screen.
  --version                                   Show version.
  --update                                    Update the database
  --detectionlist=<detectionlist>             List option
  --customlist=<customlist>                   Custom List option
  --ddc                                       Use the DDC schema for queries
  --size=<size>                               Size of the stamp in arcsec [default: 240].
  --flagdate=<flagdate>                       Flag date before which we will not request finders [default: 20230101].
  --filters=<filters>                         Filter combination to request [default: gri].
  --singlefilter=<singlefilter>               Single filter to request [default: g].
  --downloadpath=<downloadpath>               Temporary location of image downloads [default: /tmp].
  --nsigma=<nsigma>                           Specify a multiplier of the standard deviation to adjust the contrast [default: 2.0].

E.g.:
  %s ../../../../../atlas/config/config4_db1.yaml 1161549880293940400 --ddc --update
  %s ../../../../../atlas/config/config4_db1.yaml --detectionlist=2 --ddc --update
  %s ../../../../../atlas/config/config4_db1.yaml 1133923001510301000 --ddc --update --nsigma=15.0
"""

import sys
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt

from gkutils.commonutils import dbConnect, calculateRMSScatter, truncate, PROCESSING_FLAGS, cleanOptions, Struct
import sys, os, shutil, errno
import logging
from psat_server_web.atlas.atlas.commonqueries import getLightcurvePoints, getNonDetections, LC_POINTS_QUERY_ATLAS_DDC, filterWhereClauseddc, FILTERS
from image_utils import addJpegCrossHairs, fitsToJpegExtension
from pstamp_utils import createFinderImage, insertPostageStampImageRecord, GROUP_TYPE_FINDER
from makeATLASStamps import getObjectsByList, getObjectsByCustomList, updateAtlasObjectProcessingFlag
from getATLASForcedPhotometry import getATLASObject
#from PIL import Image


import logging
import logging.config
## THIRD PARTY ##
import yaml
## LOCAL APPLICATION ##

level = 'WARNING'
# SETUP LOGGING
loggerConfig = """
version: 1
formatters:
      console_style:
          format: '* %(asctime)s - %(levelname)s: %(pathname)s:%(funcName)s:%(lineno)d > %(message)s'
          datefmt: '%H:%M:%S'
handlers:
      console:
          class: logging.StreamHandler
          level: """ + level + """
          formatter: console_style
          stream: ext://sys.stdout
root:
      level: """ + level + """
      handlers: [console]"""

logging.config.dictConfig(yaml.load(loggerConfig))
logger = logging.getLogger(__name__)


#logger = logging.getLogger(__name__)
# Give Dave's code a logger - even if it's a dummy one.

def getSpecifiedObjects(conn, objectIds, processingFlags = PROCESSING_FLAGS['reffinders']):
    """getSpecifiedObjects.

    Args:
        conn:
        objectIds:
        processingFlags:
    """
    import MySQLdb

    try:
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)
        objectList = []
        for id in objectIds:
            cursor.execute ("""
                select id, ra, `dec`, id 'name', followup_flag_date, atlas_designation, other_designation
                from atlas_diff_objects
                where id = %s
                and (processing_flags & %s = 0 or processing_flags is null)
            """, (id, processingFlags))

            resultSet = cursor.fetchone ()

            if resultSet and len(resultSet) > 0:
                objectList.append(resultSet)

        cursor.close ()

    except MySQLdb.Error as e:
        sys.stderr.write("Error %d: %s\n" % (e.args[0], e.args[1]))
        sys.exit (1)

    return objectList


def grabPS1Finder(ra, dec, size, downloadPath, colourFilters='gri', singleFilter='g', downloader = None):
    """grabPS1Finder.

    Args:
        ra:
        dec:
        size:
        downloadPath:
        colourFilters:
        singleFilter:
    """

    if downloader is None:
        from panstamps.downloader import downloader

    # Use this for the colour Finder.

    colourFinderJPEG = None
    singleFilterFinderFITS = None
    singleFilterFinderJPEG = None

    fitsPaths, jpegPaths, colorPath = downloader(
        log=logger,
        settings=False,
        downloadDirectory=downloadPath,
        fits=False,
        jpeg=False,
        arcsecSize=size,
        filterSet=colourFilters,
        color=True,
        singleFilters=False,
        ra=ra,
        dec=dec,
        imageType="stack"  # warp | stack
        ).get()

    if len(colorPath) > 0:
        # Rename the colour finder into the right location
        colourFinderJPEG = colorPath[0]

    # rename colorPath[0] to the name of the finder.

    # Use this for the g-band Finder. We'll be storing the FITS info as well
    # as using the FITS WCS to position the annotations on the JPEGs.

    fitsPaths, jpegPaths, colorPath = downloader(
        log=logger,
        settings=False,
        downloadDirectory=downloadPath,
        fits=True,
        jpeg=False,
        arcsecSize=size,
        filterSet=singleFilter,
        color=False,
        singleFilters=True,
        ra=ra,
        dec=dec,
        imageType="stack"  # warp | stack
        ).get()

    if len(fitsPaths) > 0:
        singleFilterFinderFITS = fitsPaths[0]

    if len(jpegPaths) > 0:
        singleFilterFinderJPEG = jpegPaths[0]
        # mv jpegPaths[0] to name of object

    print(colourFinderJPEG)
    print(singleFilterFinderJPEG)
    print(singleFilterFinderFITS)

    finderFiles = {'colourJPEG': colourFinderJPEG, 'singleFilterJPEG': singleFilterFinderJPEG, 'singleFilterFITS': singleFilterFinderFITS}
    return finderFiles


def generatePS1Finders(conn, hostname, database, objectList, size, downloadPath='/tmp', colourFilters='gri', singleFilter='g', ddc = False, connSherlock = None, nsigma = 2.0):
    """generatePS1Finders.

    Args:
        conn:
        hostname:
        database:
        objectList:
        size:
        downloadPath:
        colourFilters:
        singleFilter:
        ddc:
    """
    if connSherlock is None:
        connSherlock = conn

    # 2023-07-05 KWS Instatiate the downloader
    from panstamps.downloader import downloader

    counter = 1
    for candidate in objectList:
        if ddc:
            p, recurrences = getLightcurvePoints(candidate['id'], lcQuery=LC_POINTS_QUERY_ATLAS_DDC + filterWhereClauseddc(FILTERS), conn = conn)
        else:
            p, recurrences = getLightcurvePoints(candidate['id'], conn = conn)
        objectCoords = []

        for row in recurrences:
            objectCoords.append({'RA': row.ra, 'DEC': row.dec})

        avgRa, avgDec, rms = calculateRMSScatter(objectCoords)

        if avgDec < -31.0:
            print("Sorry - PS1 finders only available above -31 degrees declination")
            continue

        print("Generating finder for object %d (%d)" % (candidate['id'], counter))
        finderFiles = grabPS1Finder(avgRa, avgDec, size, downloadPath=downloadPath, colourFilters=colourFilters, singleFilter=singleFilter, downloader = downloader)
        if finderFiles['colourJPEG'] is None:
            print("Something went wrong. Aborting creation of colour finder for this object")
            continue
        if finderFiles['singleFilterFITS'] is None:
            print("Something went wrong. Aborting creation of single filter finder for this object")
            continue

        objectInfo = {}
        if candidate['atlas_designation']:
            atlasName = candidate['atlas_designation']
            if candidate['other_designation']:
                atlasName += ' (%s)' % candidate['other_designation']
            objectInfo['name'] = atlasName
        else:
            objectInfo['name'] = candidate['id']
        objectInfo['ra'] = avgRa
        objectInfo['dec'] = avgDec
        objectInfo['filter'] = singleFilter

        # Dave uses a size of 1200 pixels for the colour jpeg if this is smaller than the FITS size
        # Hence I need to adjust the size of the pixel scale bar.

        # For the time being hard wire the colour pixel scale

        colourPixelScale = ((size / 0.25) / 1200.0) * 0.25

        addJpegCrossHairs(finderFiles['colourJPEG'], finderFiles['colourJPEG'], objectInfo = objectInfo, pixelScale = colourPixelScale, flip = False, negate = False, finder = True)

        imageDetails = createFinderImage(connSherlock, finderFiles['singleFilterFITS'], objectInfo = objectInfo, flip = False, nsigma = nsigma)

        # Relocate the finders and update the database. There is no MJD with the colour
        # jpeg, so use the single filter jpeg to relocate the image to an appropriate directory.
        imageDownloadLocation = '/' + hostname + '/images/' + database + '/' +  str(int(imageDetails['imageMJD']))

        tdate = truncate(imageDetails['imageMJD'], 3)

        imageGroupNameColour = "%d_%s_%s_%s_%s" % (candidate['id'], tdate, 'ps1%s' % colourFilters, '0', 'reffinder')
        imageGroupNameMono = "%d_%s_%s_%s_%s" % (candidate['id'], tdate, 'ps1%s' % singleFilter, '0', 'reffinder')

        # Create the relevant MJD directory under the images root
        if not os.path.exists(imageDownloadLocation):
           try:
               os.makedirs(imageDownloadLocation)
           except OSError as e:
               if e.errno == errno.EEXIST and os.path.isdir(imageDownloadLocation):
                   pass
               else:
                   raise
           os.chmod(imageDownloadLocation, 0o775)

        shutil.move(finderFiles['colourJPEG'], imageDownloadLocation + '/' + imageGroupNameColour + '.jpeg')
        shutil.move(finderFiles['singleFilterFITS'], imageDownloadLocation + '/' + imageGroupNameMono + '.fits')
        shutil.move(fitsToJpegExtension(finderFiles['singleFilterFITS']), imageDownloadLocation + '/' + imageGroupNameMono + '.jpeg')
        (imageId, imageGroupId) = insertPostageStampImageRecord(conn, imageGroupNameColour, os.path.basename(finderFiles['colourJPEG']), imageDetails['imageMJD'], 0, colourFilters, None,None, groupType = GROUP_TYPE_FINDER)
        (imageId, imageGroupId) = insertPostageStampImageRecord(conn, imageGroupNameMono, os.path.basename(finderFiles['singleFilterFITS']), imageDetails['imageMJD'], 0, singleFilter, None,None, groupType = GROUP_TYPE_FINDER)
        # Update processing_flags for this object
        updateAtlasObjectProcessingFlag(conn, candidate, processingFlag = PROCESSING_FLAGS['reffinders'])
        counter += 1

    return

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
    options = Struct(**opts)
        
    configFile = options.configfile
    
    import yaml
    with open(configFile) as yaml_file:
        config = yaml.load(yaml_file)

    username = config['databases']['local']['username']
    password = config['databases']['local']['password']
    database = config['databases']['local']['database']
    hostname = config['databases']['local']['hostname']

    susername = config['databases']['sherlock']['username']
    spassword = config['databases']['sherlock']['password']
    sdatabase = config['databases']['sherlock']['database']
    shostname = config['databases']['sherlock']['hostname']

    detectionList = 1
    customList = None

    conn = dbConnect(hostname, username, password, database)
    # 2023-03-13 KWS MySQLdb disables autocommit by default. Switch it on globally.
    conn.autocommit(True)

    connSherlock = dbConnect(shostname, susername, spassword, sdatabase)

    objectList = []

    update = options.update
    size = int(options.size)

    flagDate = '2015-12-20'
    if options.flagdate is not None:
        try:    
            flagDate = '%s-%s-%s' % (options.flagdate[0:4], options.flagdate[4:6], options.flagdate[6:8])
        except:
            flagDate = '2015-12-20'

    if options.candidate is not None and len(options.candidate) > 0:
        for cand in options.candidate:
            obj = getATLASObject(conn, objectId = int(cand))
            if obj:
                objectList.append(obj)
    else:

        if options.customlist is not None:
            if int(options.customlist) > 0 and int(options.customlist) < 100:
                customList = int(options.customlist)
                objectList = getObjectsByCustomList(conn, customList, processingFlags = PROCESSING_FLAGS['reffinders'])
            else:
                print("The list must be between 1 and 100 inclusive.  Exiting.")
                sys.exit(1)
        else:
            if options.detectionlist is not None:
                if int(options.detectionlist) >= 0 and int(options.detectionlist) < 9:
                    detectionList = int(options.detectionlist)
                    objectList = getObjectsByList(conn, listId = detectionList, dateThreshold = flagDate, processingFlags = PROCESSING_FLAGS['reffinders'])
                else:
                    print("The list must be between 0 and 6 inclusive.  Exiting.")
                    sys.exit(1)

    print("LENGTH OF OBJECTLIST = ", len(objectList))

    PSSImageRootLocation = '/' + hostname + '/images/' + database


    generatePS1Finders(conn, hostname, database, objectList, int(options.size), downloadPath=options.downloadpath, colourFilters=options.filters, singleFilter=options.singlefilter, ddc = options.ddc, connSherlock = connSherlock, nsigma = float(options.nsigma))

    conn.close()
    connSherlock.close()


if __name__=='__main__':
    main()

