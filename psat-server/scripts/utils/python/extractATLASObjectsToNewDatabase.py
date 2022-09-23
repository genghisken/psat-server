#!/usr/bin/env python
# Get which list we want to migrate.  We assume that the new database will be on the same
# database server as the exising DB.  Otherwise the SELECT/INSERT queries will become very
# complicated.


# Extract all Confirmed, Good into a new database.
# Requires new empty database. NOTE: This is only half the problem.  We need another script
# to extract location maps and downloaded images.

# Important to include ALL the original CMF files (tcs_cmf_metadata).  This is because when
# re-generating light curves, we need to know if a skycell has been
# observed previously. (Also makes the query simpler!!)

# NOTE: This script should NOT be run by root.  It's too dangerous to allow root to run
#       this script. The potential for accidentially deleting the source database is very
#       high.

"""Move ATLAS objects in specified list to another database.  For safety do NOT
use a destination database user that also has write access to the source database.
Assumes:
  1. The destination user has at least SELECT privileges on the source database.
  2. The databases are in the SAME MySQL server instance.

Usage:
  %s <username> <password> <database> <hostname> <sourceschema> [<candidates>...] [--truncate] [--ddc] [--list=<listid>] [--flagdate=<flagdate>] [--copyimages]
  %s (-h | --help)
  %s --version

Options:
  -h --help              Show this screen.
  --version              Show version.
  --list=<listid>        List to be migrated [default: 2].
  --flagdate=<flagdate>  Flag date threshold beyond which we will select objects [default: 20170920].
  --ddc                  Assume the DDC schema.
  --truncate             Truncate the database tables. Default is NOT to truncate.
  --copyimages           Copy the images as well (extremely time consuming).

"""
import sys
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt
import os, MySQLdb, shutil, re
from gkutils.commonutils import dbConnect, find, Struct, cleanOptions
from psat_server_web.atlas.atlas.commonqueries import getNonDetectionsUsingATLASFootprint, LC_POINTS_QUERY_ATLAS_DDC, ATLAS_METADATADDC, filterWhereClauseddc, FILTERS

IMAGEROOT_SOURCE = '/db4/images/'
IMAGEROOT_DESTINATION = '/db4/images/'

FIELD_REGEX = '^([a-zA-Z0-9]+)\.'
COMPILED_FIELD_REGEX = re.compile(FIELD_REGEX)
SKYCELL_REGEX = 'skycell\.([0-9]+(\.[0-9]+){0,1})(\.[S|W]S){0,1}\.dif\.'
COMPILED_SKYCELL_REGEX = re.compile(SKYCELL_REGEX)

class EmptyRecurreces:
    pass


# 2013-10-21 KWS Added a date threshold.
def getATLASObjects(conn, listId = 4, objectType = -1, dateThreshold = '2013-06-01', objectId = None):
    """getATLASObjects.

    Args:
        conn:
        listId:
        objectType:
        dateThreshold:
        objectId:
    """

    try:
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)
        if objectId is None:
            cursor.execute ("""
                select id, followup_id, ra, `dec`, atlas_designation 'name', object_classification, followup_flag_date
                from atlas_diff_objects
                where detection_list_id = %s
                and followup_flag_date > %s
                order by followup_id
            """, (listId, dateThreshold))
            resultSet = cursor.fetchall ()
        else:
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



# We need all the object recurrences for the object.

def getObjectInfo(conn, objectId):
    """getObjectInfo.

    Args:
        conn:
        objectId:
    """

    try:
        cursor = conn.cursor (MySQLdb.cursors.DictCursor)

        cursor.execute ("""
            SELECT d.ra RA,
                   d.dec 'DEC',
                   d.atlas_object_id,
                   m.filter Filter,
                   m.mjd_obs MJD,
                   m.filename Filename,
                   m.object field,
                   d.tphot_id,
                   d.mag,
                   d.dm,
                   m.exptime,
                   m.mag5sig,
                   d.id,
                   d.htm16ID
            FROM atlas_diff_detections d, atlas_metadata m
            where d.atlas_object_id=%s
            and d.atlas_metadata_id = m.id
            ORDER by MJD
        """, (objectId,))
        resultSet = cursor.fetchall ()

        cursor.close ()

    except MySQLdb.Error as e:
        print("Error %d: %s" % (e.args[0], e.args[1]))
        sys.exit (1)


    return resultSet


# 2017-06-19 KWS Get object info as defined in DDC files.
# 2017-10-03 KWS Only pick up POSITIVE FLUX (det != 5).
# 2018-03-19 KWS Apparently, det != 5 is not quite enough to exclude negative flux!
# 2018-05-03 KWS Include an option to pick up the negative flux detections (e.g.
#                for calculating average RA and Dec). This will also get all the
#                deprecated detections.
def getObjectInfoddc(conn, objectId, negativeFlux = False):
    """getObjectInfoddc.

    Args:
        conn:
        objectId:
        negativeFlux:
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
        print("Error %d: %s" % (e.args[0], e.args[1]))
        sys.exit (1)


    return resultSet


def getSpecifiedObjects(conn, objectIds):
    """

    :param conn: database connection
    :param objectIds: 
    :return resultSet: The tuple of object dicts

    """
    import MySQLdb

    try:
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)
        objectList = []
        for id in objectIds:
            cursor.execute ("""
                select id, ra, `dec`, followup_flag_date, atlas_designation
                from atlas_diff_objects
                where id = %s
            """ , (id,))

            resultSet = cursor.fetchone ()

            if resultSet is not None and len(resultSet) > 0:
                objectList.append(resultSet)

        cursor.close ()

    except MySQLdb.Error as e:
        sys.stderr.write("Error %d: %s\n" % (e.args[0], e.args[1]))
        sys.exit (1)

    return objectList


def getObjectImages(conn, objectId, schemaName):
    """Need to intelligently acquire the object images"""

    try:
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)
        query = """ select image_filename
                    from %s.tcs_postage_stamp_images
                    where image_filename like '%s%%'
                """ % (schemaName, objectId)

        cursor.execute(query)

        resultSet = cursor.fetchall ()
        cursor.close ()

    except MySQLdb.Error as e:
        print("Error %d: %s" % (e.args[0], e.args[1]))
        sys.exit (1)

    return resultSet


# First of all, delete the contents of the new database. It's CRUCIAL that we delete the
# correct database!!  For this reason, the db credentials will be hard-wired into this code.

def truncateTable(conn, tableName, schemaName = 'atlas4public'):
    """This function is only safe if the only the schema owner can truncate the table"""
    try:
        cursor = conn.cursor(MySQLdb.cursors.Cursor)
        ddl = "truncate table %s.%s" % (schemaName, tableName)
        print(ddl)
        cursor.execute(ddl)
        cursor.close ()

    except MySQLdb.Error as e:
        print("Error %d: %s" % (e.args[0], e.args[1]))

    return 0


# 2017-05-10 KWS Added tcs_object_comments
def truncateAllTables(conn, publicSchema):

    truncateTable(conn, 'tcs_cross_matches', publicSchema)
    truncateTable(conn, 'tcs_cross_matches_external', publicSchema)
    truncateTable(conn, 'tcs_images', publicSchema)
    truncateTable(conn, 'tcs_image_groups', publicSchema)
    truncateTable(conn, 'tcs_postage_stamp_images', publicSchema)
    truncateTable(conn, 'tcs_latest_object_stats', publicSchema)
    truncateTable(conn, 'tcs_object_comments', publicSchema)
    truncateTable(conn, 'tcs_object_groups', publicSchema)
    truncateTable(conn, 'tcs_object_group_definitions', publicSchema)
    truncateTable(conn, 'atlas_diff_objects', publicSchema)
    truncateTable(conn, 'atlas_diff_detections', publicSchema)
    truncateTable(conn, 'atlas_diff_moments', publicSchema)
    truncateTable(conn, 'atlas_metadata', publicSchema)
    truncateTable(conn, 'atlas_metadataddc', publicSchema)
    truncateTable(conn, 'atlas_detectionsddc', publicSchema)
    truncateTable(conn, 'atlas_forced_photometry', publicSchema)
    truncateTable(conn, 'sherlock_classifications', publicSchema)
    truncateTable(conn, 'sherlock_crossmatches', publicSchema)


def insertAllRecords(conn, tableName, privateSchema, publicSchema):
    """For meta files we need ALL of them to allow plotting of arrows on lightcurves"""

    try:
        cursor = conn.cursor(MySQLdb.cursors.Cursor)
        ddl = "insert into %s.%s select * from %s.%s" % (publicSchema, tableName, privateSchema, tableName)
        cursor.execute(ddl)
        cursor.close ()

    except MySQLdb.Error as e:
        print("Error %d: %s" % (e.args[0], e.args[1]))

    return 0


def insertRecord(conn, tableName, objectId, idColumn, privateSchema, publicSchema):

    try:
        cursor = conn.cursor(MySQLdb.cursors.Cursor)
        ddl = "insert into %s.%s select * from %s.%s where %s = %s" % (publicSchema, tableName, privateSchema, tableName, idColumn, objectId)
        print(ddl)
        cursor.execute(ddl)
        cursor.close ()

    except MySQLdb.Error as e:
        print("Error %d: %s" % (e.args[0], e.args[1]))

    return 0


def insertRecordLike(conn, tableName, objectId, idColumn, privateSchema, publicSchema):

    try:
        cursor = conn.cursor(MySQLdb.cursors.Cursor)
        ddl = "insert into %s.%s select * from %s.%s where %s like '%s%%'" % (publicSchema, tableName, privateSchema, tableName, idColumn, objectId)
        print(ddl)
        cursor.execute(ddl)
        cursor.close ()

    except MySQLdb.Error as e:
        print("Error %d: %s" % (e.args[0], e.args[1]))

    return 0


def removeAllImagesAndLocationMaps(publicSchema):

    # Pick up all the subdirectories that are MJDs.
    directories = find('5[0-9][0-9][0-9][0-9]', IMAGEROOT_DESTINATION + publicSchema, directoriesOnly = True)
    for dir in directories:
        shutil.rmtree(dir)




def copyImages(conn, objectId, privateSchema, publicSchema):

    imageFilenames = getObjectImages(conn, objectId, privateSchema)
    for filename in imageFilenames:
        # Copy the files over one by one. Don't worry if they're not there.

        mjd = filename['image_filename'].split('_')[1]
        mjd = mjd.split('.')[0]
        imageSourceFilename = IMAGEROOT_SOURCE + privateSchema + '/' + mjd + '/' + filename['image_filename'] + '.jpeg'
        imageDestinationDirectoryName = IMAGEROOT_DESTINATION + publicSchema + '/' + mjd
        imageDestinationFilename = imageDestinationDirectoryName + '/' + filename['image_filename'] + '.jpeg'

        if not os.path.exists(imageDestinationDirectoryName):
            os.makedirs(imageDestinationDirectoryName)

        try:
            if not os.path.exists(imageDestinationFilename):
                # Don't need to copy files we already have.
                shutil.copy2(imageSourceFilename, imageDestinationFilename)
        except IOError as e:
            print(e)


# 2015-04-24 KWS Occasionally we'd like to publish individual objects or a small sublist
#                e.g. when we announce a discovery ATel.  Hence allow publishing of the
#                sublist without trashing the entire database.
# 2017-05-10 KWS Added the new tcs_object_comments table
def migrateData(conn, connPrivateReadonly, objectList, publicSchema, privateSchema, truncateTables = False, ddc = False, copyImages = False):

    if truncateTables:
        truncateAllTables(conn, publicSchema)
        removeAllImagesAndLocationMaps(publicSchema)

    # Now add the objects one-at-a time.  The advantage of doing it this way is
    # that we can veto publication of individual objects if necessary.

    counter = 1
    exposures = []
    listLength = len(objectList)
    for object in objectList:
        print("Migrating object %d (%d of %d)" % (object['id'], counter, listLength))
        insertRecord(conn, 'atlas_diff_objects', object['id'], 'id', privateSchema, publicSchema)
        if ddc:
            insertRecord(conn, 'atlas_detectionsddc', object['id'], 'atlas_object_id', privateSchema, publicSchema)
        else:
            insertRecord(conn, 'atlas_diff_detections', object['id'], 'atlas_object_id', privateSchema, publicSchema)
        insertRecord(conn, 'atlas_forced_photometry', object['id'], 'atlas_object_id', privateSchema, publicSchema)
        insertRecord(conn, 'tcs_cross_matches', object['id'], 'transient_object_id', privateSchema, publicSchema)
        insertRecord(conn, 'tcs_latest_object_stats', object['id'], 'id', privateSchema, publicSchema)
        insertRecord(conn, 'tcs_cross_matches_external', object['id'], 'transient_object_id', privateSchema, publicSchema)
        insertRecord(conn, 'tcs_object_comments', object['id'], 'transient_object_id', privateSchema, publicSchema)
        insertRecord(conn, 'sherlock_classifications', object['id'], 'transient_object_id', privateSchema, publicSchema)
        insertRecord(conn, 'sherlock_crossmatches', object['id'], 'transient_object_id', privateSchema, publicSchema)
        insertRecord(conn, 'tcs_object_groups', object['id'], 'transient_object_id', privateSchema, publicSchema)
        insertRecordLike(conn, 'tcs_image_groups', object['id'], 'name', privateSchema, publicSchema)
        insertRecordLike(conn, 'tcs_postage_stamp_images', object['id'], 'image_filename', privateSchema, publicSchema)
        insertRecordLike(conn, 'tcs_images', object['id'], 'target', privateSchema, publicSchema)

        # Create a dummy recurrence so we can reuse existing code.
        recurrence = EmptyRecurreces()
        recurrence.ra = object['ra']
        recurrence.dec = object['dec']
        recurrence.mjd = 50000
        recurrence.atlas_metadata_id = 0
        recurrences = [recurrence]

        if ddc:
            b, blanks, lastNonDetection = getNonDetectionsUsingATLASFootprint(recurrences, conn = connPrivateReadonly, ndQuery=ATLAS_METADATADDC, filterWhereClause = filterWhereClauseddc, catalogueName = 'atlas_metadataddc')
        else:
            b, blanks, lastNonDetection = getNonDetectionsUsingATLASFootprint(recurrences, conn = connPrivateReadonly)

        for row in blanks:
            exposures.append(row.expname)

        if not ddc:
            # Need to grab the detection ids for the moments insert
            objectInfo = getObjectInfo(conn, object['id'])
            for info in objectInfo:
                detectionIds.append(info['id'])

        if copyImages:
            print("Copying images...")
            copyImages(conn, object['id'], privateSchema, publicSchema)
        counter += 1

    uniqueExposures = sorted(list(set(exposures)))
    for exp in uniqueExposures:
        # Now grab all the exposure information.
        if ddc:
            insertRecord(conn, 'atlas_metadataddc', '\'%s\'' % (exp), 'obs', privateSchema, publicSchema)
        else:
            insertRecord(conn, 'atlas_metadata', '\'%s\'' % (exp), 'expname', privateSchema, publicSchema)

    if not ddc:
        # Insert the moments entries.
        for detId in set(detectionIds):
            insertRecord(conn, 'atlas_diff_moments', detId, 'detection_id', privateSchema, publicSchema)



def main(argv = None):
    opts = docopt(__doc__, version='0.1')
    opts = cleanOptions(opts)

    # Use utils.Struct to convert the dict into an object for compatibility with old optparse code.
    options = Struct(**opts)

    detectionList = 2

    truncateTables = options.truncate

    print("Truncate tables =", truncateTables)

    if 'public' not in options.database:
        sys.exit("ONLY the public database credentials should be entered")

    # If the list isn't specified assume it's the Eyeball List.
    if options.list is not None:
        try:
            detectionList = int(options.list)
            if detectionList < 1 or detectionList > 6:
                print("Detection list must be between 1 and 6")
                return 1
        except ValueError as e:
            sys.exit("Detection list must be an integer")

    if options.flagdate is not None:
        try:
            dateThreshold = '%s-%s-%s' % (options.flagdate[0:4], options.flagdate[4:6], options.flagdate[6:8])
        except:
            dateThreshold = '2017-09-20'

    # Connect to the database to which we want to WRITE. The user should have READ ONLY
    # (i.e. SELECT only) access to the SOURCE database.
    conn = dbConnect(options.hostname, options.username, options.password, options.database)
    if not conn:
        print("Cannot connect to the public database")
        return 1

    connPrivateReadonly = dbConnect(options.hostname, options.username, options.password, options.sourceschema)
    if not connPrivateReadonly:
        print("Cannot connect to the private database")
        return 1

    candidateList = []

    # Supplied candidates override the specified list
    if options.candidates:
        for candidate in options.candidates:
            candidateList.append(int(candidate))
        candidateList = getSpecifiedObjects(connPrivateReadonly, candidateList)

    else:
        candidateList = getATLASObjects(connPrivateReadonly, listId = detectionList)

    print("Length of list = %d)" % len(candidateList))
    migrateData(conn, connPrivateReadonly, candidateList, options.database, options.sourceschema, truncateTables = truncateTables, ddc = options.ddc, copyImages = options.copyimages)

    conn.close ()
    connPrivateReadonly.close ()

    return



if __name__ == '__main__':
    main()
