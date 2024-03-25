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
  %s <username> <password> <database> <hostname> <sourceschema> [<candidates>...] [--truncate] [--ddc] [--list=<listid>] [--flagdate=<flagdate>] [--copyimages] [--dumpfile=<dumpfile>] [--nocreateinfo] [--djangofile=<djangofile>] [--imagessource=<imagessource>] [--imagesdest=<imagesdest>]
  %s (-h | --help)
  %s --version

Options:
  -h --help                      Show this screen.
  --version                      Show version.
  --list=<listid>                List to be migrated [default: 2].
  --flagdate=<flagdate>          Flag date threshold beyond which we will select objects [default: 20170920].
  --ddc                          Assume the DDC schema.
  --truncate                     Truncate the database tables. Default is NOT to truncate.
  --copyimages                   Copy the images as well (extremely time consuming).
  --nocreateinfo                 When dumping db tables from large db, skip the create statements (for inhomogenious backends situation).
  --dumpfile=<dumpfile>          Filename of dumped data [default: /home/atls/big_tables.sql].
  --djangofile=<djangofile>      Filename of dumped Django data [default: /home/atls/django_schema.sql].
  --imagessource=<imagessource>  Source root location of the images [default: /db4/images/].
  --imagesdest=<imagesdest>      Destination location for extracted images [default: /db4/images/].

E.g.:
  %s publicuser publicpass public db1 atlas4 --ddc --list=5 --copyimages
"""
import sys
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0])
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
def truncateAllTables(conn, newSchema):

    truncateTable(conn, 'tcs_catalogue_tables', newSchema)
    truncateTable(conn, 'tcs_cross_matches', newSchema)
    truncateTable(conn, 'tcs_cross_matches_external', newSchema)
    truncateTable(conn, 'tcs_cmf_metadata', newSchema)
    truncateTable(conn, 'tcs_detection_lists', newSchema)
    truncateTable(conn, 'tcs_transient_objects', newSchema)
    truncateTable(conn, 'tcs_transient_reobservations', newSchema)
    truncateTable(conn, 'tcs_images', newSchema)
    truncateTable(conn, 'tcs_image_groups', newSchema)
    truncateTable(conn, 'tcs_postage_stamp_images', newSchema)
    truncateTable(conn, 'tcs_processing_status', newSchema)
    truncateTable(conn, 'tcs_latest_object_stats', newSchema)
    truncateTable(conn, 'tcs_object_comments', newSchema)
    truncateTable(conn, 'tcs_object_groups', newSchema)
    truncateTable(conn, 'tcs_object_group_definitions', newSchema)
    truncateTable(conn, 'atlas_diff_objects', newSchema)
    truncateTable(conn, 'atlas_diff_detections', newSchema)
    truncateTable(conn, 'atlas_diff_moments', newSchema)
    truncateTable(conn, 'atlas_metadata', newSchema)
    truncateTable(conn, 'atlas_metadataddc', newSchema)
    truncateTable(conn, 'atlas_detectionsddc', newSchema)
    truncateTable(conn, 'atlas_forced_photometry', newSchema)
    truncateTable(conn, 'atlas_stacked_forced_photometry', newSchema)
    truncateTable(conn, 'tcs_gravity_event_annotations', newSchema)
    truncateTable(conn, 'tcs_gravity_events', newSchema)
    truncateTable(conn, 'atlas_heatmaps', newSchema)
    truncateTable(conn, 'sherlock_classifications', newSchema)
    truncateTable(conn, 'sherlock_crossmatches', newSchema)
    truncateTable(conn, 'atlas_diff_logs', newSchema)
    truncateTable(conn, 'atlas_diff_subcells', newSchema)
    truncateTable(conn, 'atlas_diff_subcell_logs', newSchema)
    truncateTable(conn, 'tcs_gravity_alerts', newSchema)
    truncateTable(conn, 'tcs_vra_scores', newSchema)

def insertAllRecords(conn, tableName, sourceReadOnlySchema, newSchema):
    """For meta files we need ALL of them to allow plotting of arrows on lightcurves"""

    try:
        cursor = conn.cursor(MySQLdb.cursors.Cursor)
        ddl = "replace into %s.%s select * from %s.%s" % (newSchema, tableName, sourceReadOnlySchema, tableName)
        cursor.execute(ddl)
        cursor.close ()

    except MySQLdb.Error as e:
        print("Error %d: %s" % (e.args[0], e.args[1]))

    return 0


def insertRecord(conn, tableName, objectId, idColumn, sourceReadOnlySchema, newSchema):

    try:
        cursor = conn.cursor(MySQLdb.cursors.Cursor)
        ddl = "insert into %s.%s select * from %s.%s where %s = %s" % (newSchema, tableName, sourceReadOnlySchema, tableName, idColumn, objectId)
        print(ddl)
        cursor.execute(ddl)
        cursor.close ()

    except MySQLdb.Error as e:
        print("Error %d: %s" % (e.args[0], e.args[1]))

    return 0


def insertRecordLike(conn, tableName, objectId, idColumn, sourceReadOnlySchema, newSchema):

    try:
        cursor = conn.cursor(MySQLdb.cursors.Cursor)
        ddl = "insert into %s.%s select * from %s.%s where %s like '%s%%'" % (newSchema, tableName, sourceReadOnlySchema, tableName, idColumn, objectId)
        print(ddl)
        cursor.execute(ddl)
        cursor.close ()

    except MySQLdb.Error as e:
        print("Error %d: %s" % (e.args[0], e.args[1]))

    return 0


def removeAllImagesAndLocationMaps(newSchema, imageRootDestination):

    # Pick up all the subdirectories that are MJDs.
    if imageRootDestination is not None:
        directories = find('[56][0-9][0-9][0-9][0-9]', imageRootDestination + newSchema, directoriesOnly = True)
        for dir in directories:
            shutil.rmtree(dir)




def copyImages(conn, objectId, sourceReadOnlySchema, newSchema, imageRootSource, imageRootDestination):

    imageFilenames = getObjectImages(conn, objectId, sourceReadOnlySchema)
    for filename in imageFilenames:
        # Copy the files over one by one. Don't worry if they're not there.

        # 2023-03-06 KWS For South Africa, the MJD of the exposure is usually 1
        #                night BEFORE the designated night number (since it straddles
        #                midnight UTC). So if we have exposure name in the file, get
        #                the MJD from the exposure name. Otherwise get it from the MJD
        #                embedded in the image_filename (as before).
        if any([x in filename['image_filename'] for x in ['01a', '02a', '03a', '04a']]):
            mjd = filename['image_filename'].split('_')[2][3:8]
        else:
            # Use the old way to get the MJD - relevant for finding charts.
            mjd = filename['image_filename'].split('_')[1]
            mjd = mjd.split('.')[0]
        imageDestinationDirectoryName = imageRootDestination + newSchema + '/' + mjd

        imageSourceFilenameJpeg = imageRootSource + sourceReadOnlySchema + '/' + mjd + '/' + filename['image_filename'] + '.jpeg'
        imageDestinationFilenameJpeg = imageDestinationDirectoryName + '/' + filename['image_filename'] + '.jpeg'
        imageSourceFilenameFits = imageRootSource + sourceReadOnlySchema + '/' + mjd + '/' + filename['image_filename'] + '.fits'
        imageDestinationFilenameFits = imageDestinationDirectoryName + '/' + filename['image_filename'] + '.fits'

        if not os.path.exists(imageDestinationDirectoryName):
            os.makedirs(imageDestinationDirectoryName, exist_ok=True)

        try:
            if not os.path.exists(imageDestinationFilenameJpeg):
                # Don't need to copy files we already have.
                shutil.copy2(imageSourceFilenameJpeg, imageDestinationFilenameJpeg)
            if not os.path.exists(imageDestinationFilenameFits):
                # Don't need to copy files we already have.
                shutil.copy2(imageSourceFilenameFits, imageDestinationFilenameFits)
        except IOError as e:
            print(e)


# 2015-04-24 KWS Occasionally we'd like to publish individual objects or a small sublist
#                e.g. when we announce a discovery ATel.  Hence allow publishing of the
#                sublist without trashing the entire database.
# 2017-05-10 KWS Added the new tcs_object_comments table
# 2024-03-04 KWS Added the new tcs_vra_scores table
def migrateData(conn, connPrivateReadonly, objectList, newSchema, sourceReadOnlySchema, ddc = False, copyimages = False, imageRootSource = None, imageRootDestination = None):

    # Now add the objects one-at-a time.  The advantage of doing it this way is
    # that we can veto publication of individual objects if necessary.

    counter = 1
    exposures = []
    listLength = len(objectList)
    for object in objectList:
        print("Migrating object %d (%d of %d)" % (object['id'], counter, listLength))
        insertRecord(conn, 'tcs_transient_objects', object['id'], 'id', sourceReadOnlySchema, newSchema)
        insertRecord(conn, 'tcs_transient_reobservations', object['id'], 'transient_object_id', sourceReadOnlySchema, newSchema)
        insertRecord(conn, 'atlas_diff_objects', object['id'], 'id', sourceReadOnlySchema, newSchema)
        insertRecord(conn, 'atlas_detectionsddc', object['id'], 'atlas_object_id', sourceReadOnlySchema, newSchema)
        insertRecord(conn, 'atlas_diff_detections', object['id'], 'atlas_object_id', sourceReadOnlySchema, newSchema)
        insertRecord(conn, 'atlas_forced_photometry', object['id'], 'atlas_object_id', sourceReadOnlySchema, newSchema)
        insertRecord(conn, 'atlas_stacked_forced_photometry', object['id'], 'atlas_object_id', sourceReadOnlySchema, newSchema)
        insertRecord(conn, 'tcs_cross_matches', object['id'], 'transient_object_id', sourceReadOnlySchema, newSchema)
        insertRecord(conn, 'tcs_latest_object_stats', object['id'], 'id', sourceReadOnlySchema, newSchema)
        insertRecord(conn, 'tcs_cross_matches_external', object['id'], 'transient_object_id', sourceReadOnlySchema, newSchema)
        insertRecord(conn, 'tcs_object_comments', object['id'], 'transient_object_id', sourceReadOnlySchema, newSchema)
        insertRecord(conn, 'sherlock_classifications', object['id'], 'transient_object_id', sourceReadOnlySchema, newSchema)
        insertRecord(conn, 'sherlock_crossmatches', object['id'], 'transient_object_id', sourceReadOnlySchema, newSchema)
        insertRecord(conn, 'tcs_object_groups', object['id'], 'transient_object_id', sourceReadOnlySchema, newSchema)
        insertRecordLike(conn, 'tcs_image_groups', object['id'], 'name', sourceReadOnlySchema, newSchema)
        insertRecordLike(conn, 'tcs_postage_stamp_images', object['id'], 'image_filename', sourceReadOnlySchema, newSchema)
        insertRecordLike(conn, 'tcs_images', object['id'], 'target', sourceReadOnlySchema, newSchema)
        insertRecord(conn, 'tcs_gravity_event_annotations', object['id'], 'transient_object_id', sourceReadOnlySchema, newSchema)
        insertRecord(conn, 'tcs_zooniverse_scores', object['id'], 'transient_object_id', sourceReadOnlySchema, newSchema)
        # 2024-03-22 KWS Added tcs_forced_photometry.
        insertRecord(conn, 'tcs_forced_photometry', object['id'], 'transient_object_id', sourceReadOnlySchema, newSchema)
        # 2024-03-09 KWS Added tcs_vra_scores.
        insertRecord(conn, 'tcs_vra_scores', object['id'], 'transient_object_id', sourceReadOnlySchema, newSchema)

        # Create a dummy recurrence so we can reuse existing code.
        recurrence = EmptyRecurreces()
        recurrence.ra = object['ra']
        recurrence.dec = object['dec']
        recurrence.mjd = 50000
        recurrence.atlas_metadata_id = 0
        recurrences = [recurrence]

#        if ddc:
#            b, blanks, lastNonDetection = getNonDetectionsUsingATLASFootprint(recurrences, conn = connPrivateReadonly, ndQuery=ATLAS_METADATADDC, filterWhereClause = filterWhereClauseddc, catalogueName = 'atlas_metadataddc')
#        else:
#            b, blanks, lastNonDetection = getNonDetectionsUsingATLASFootprint(recurrences, conn = connPrivateReadonly)
#
#        for row in blanks:
#            exposures.append(row.expname)
#
        if not ddc:
            # Need to grab the detection ids for the moments insert
            objectInfo = getObjectInfo(conn, object['id'])
            for info in objectInfo:
                detectionIds.append(info['id'])

        if copyimages and imageRootSource is not None and imageRootDestination is not None:
            print("Copying images...")
            copyImages(conn, object['id'], sourceReadOnlySchema, newSchema, imageRootSource, imageRootDestination)
        counter += 1

#    uniqueExposures = sorted(list(set(exposures)))
#    for exp in uniqueExposures:
#        # Now grab all the exposure information.
#        if ddc:
#            insertRecord(conn, 'atlas_metadataddc', '\'%s\'' % (exp), 'obs', sourceReadOnlySchema, newSchema)
#        else:
#            insertRecord(conn, 'atlas_metadata', '\'%s\'' % (exp), 'expname', sourceReadOnlySchema, newSchema)

    if not ddc:
        # Insert the moments entries.
        for detId in set(detectionIds):
            insertRecord(conn, 'atlas_diff_moments', detId, 'detection_id', sourceReadOnlySchema, newSchema)



def main(argv = None):
    opts = docopt(__doc__, version='0.1')
    opts = cleanOptions(opts)

    # Use utils.Struct to convert the dict into an object for compatibility with old optparse code.
    options = Struct(**opts)

    detectionList = 2

    print("Truncate tables =", options.truncate)

    # 2023-03-27 KWS This code needs to be modified to CHECK that the credentials
    #                of the new schema has READ ONLY access to the source schema.
    #                The script should ABORT if the new schema credentials have
    #                write access to the old schema.
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

    # 2023-03-07 KWS MySQLdb disables autocommit by default. Switch it on globally.
    conn.autocommit(True)

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

    # 2022-11-24 Move the table truncation outside the migrateData function so that we can do this also in the multiprocessed version.
    if options.truncate:
        print('Truncating the tables...')
        truncateAllTables(conn, options.database)
        print('Removing the images...')
        removeAllImagesAndLocationMaps(options.database, options.imagesdest)

        # 2024-03-09 KWS Commented out big tables that are just easier to dump and retrieve.
        # First of all insert the complete tables we definitely want to keep.
        #print('Inserting data into tcs_cmf_metadata...')
        #insertAllRecords(conn, 'tcs_cmf_metadata', options.sourceschema, options.database)
        #print('Inserting data into atlas_metadata...')
        #insertAllRecords(conn, 'atlas_metadata', options.sourceschema, options.database)
        #print('Inserting data into atlas_metadataddc...')
        #insertAllRecords(conn, 'atlas_metadataddc', options.sourceschema, options.database)
        print('Inserting data into tcs_gravity_events...')
        insertAllRecords(conn, 'tcs_gravity_events', options.sourceschema, options.database)
        print('Inserting data into atlas_heatmaps...')
        insertAllRecords(conn, 'atlas_heatmaps', options.sourceschema, options.database)
        print('Inserting data into tcs_object_group_definitions...')
        insertAllRecords(conn, 'tcs_object_group_definitions', options.sourceschema, options.database)
        print('Inserting data into atlas_diff_logs...')
        insertAllRecords(conn, 'atlas_diff_logs', options.sourceschema, options.database)
        print('Inserting data into tcs_processing_status...')
        insertAllRecords(conn, 'tcs_processing_status', options.sourceschema, options.database)
        print('Inserting data into tcs_catalogue_tables...')
        insertAllRecords(conn, 'tcs_catalogue_tables', options.sourceschema, options.database)
        print('Inserting data into tcs_tns_requests...')
        insertAllRecords(conn, 'tcs_tns_requests', options.sourceschema, options.database)
        print('Inserting data into tcs_detection_lists...')
        insertAllRecords(conn, 'tcs_detection_lists', options.sourceschema, options.database)
        print('Inserting data into tcs_gravity_alerts...')
        insertAllRecords(conn, 'tcs_gravity_alerts', options.sourceschema, options.database)

        # 2024-03-14 KWS Added authtoken_token. 
        print('Extracting all the Django relevant tables into a dump file. Requires SELECT and LOCK TABLE access to sourceschema.')
        cmd = 'mysqldump -u%s --password=%s %s -h %s --no-tablespaces auth_group auth_group_permissions auth_permission auth_user auth_user_groups auth_user_user_permissions authtoken_token django_admin_log django_content_type django_migrations django_session django_site > %s' % (options.username, options.password, options.sourceschema, options.hostname, options.djangofile)
        os.system(cmd)

        print('Importing the Django tables from the %s schema.' % options.sourceschema)
        cmd = 'mysql -u%s --password=%s %s -h %s < %s' % (options.username, options.password, options.database, options.hostname, options.djangofile)
        os.system(cmd)

        # 2024-03-14 KWS Added tcs_cmf_metadata, atlas_metadata, atlas_metadataddc.
        print('Extracting the very large tables into a dump file. Requires SELECT and LOCK TABLE access to sourceschema.')
        noCreateInfo = ''
        if options.nocreateinfo is not None:
            noCreateInfo = '--no-create-info'
        cmd = 'mysqldump -u%s --password=%s %s -h %s %s --no-tablespaces tcs_cmf_metadata atlas_metadata atlas_metadataddc atlas_diff_subcells atlas_diff_subcell_logs > %s' % (options.username, options.password, options.sourceschema, options.hostname, noCreateInfo, options.dumpfile)
        os.system(cmd)

        print('Importing the giant tables from the %s schema.' % options.sourceschema)
        cmd = 'mysql -u%s --password=%s %s -h %s < %s' % (options.username, options.password, options.database, options.hostname, options.dumpfile)
        os.system(cmd)

        # The following tables contain over 100 million rows each. Doing a standard
        # replace into won't work.  Best to do a dump of the two tables and import
        # just before going live.
#        print('Inserting data into atlas_diff_subcells...')
#        insertAllRecords(conn, 'atlas_diff_subcells', options.sourceschema, options.database)
#        print('Inserting data into atlas_diff_subcell_logs...')
#        insertAllRecords(conn, 'atlas_diff_subcell_logs', options.sourceschema, options.database)

    print("Length of list = %d)" % len(candidateList))
    migrateData(conn, connPrivateReadonly, candidateList, options.database, options.sourceschema, ddc = options.ddc, copyimages = options.copyimages, imageRootSource = options.imagessource, imageRootDestination = options.imagesdest)

    conn.close ()
    connPrivateReadonly.close ()

    return



if __name__ == '__main__':
    main()
