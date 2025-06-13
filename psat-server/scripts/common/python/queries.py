import MySQLdb
import sys
from gkutils.commonutils import PROCESSING_FLAGS

# Temporary measure until this is properly implemented in gkutils:
PROCESSING_FLAGS['pmcheck'] = 0x2000


def getAtlasObjects(conn, listId = 4, dateThreshold = '2016-01-01', objectId = None, rbThreshold = None, processingFlags = 0):
    """getAtlasObjects.

    Args:
        conn:
        listId:
        dateThreshold:
        objectId:
        rbThreshold:
    """

    try:
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)
        if objectId is None:
            if rbThreshold is not None:
                cursor.execute ("""
                    select o.id, followup_id, ifnull(ra_avg, ra) ra, ifnull(dec_avg, `dec`) `dec`, name, object_classification, detection_list_id, mjd from (
                        select o.id, followup_id, o.ra, o.`dec`, s.ra_avg, s.dec_avg, atlas_designation 'name', object_classification, detection_list_id, m.mjd
                          from atlas_diff_objects o
                          join atlas_detectionsddc d
                            on o.detection_id = d.id
                          join atlas_metadataddc m
                            on m.id = d.atlas_metadata_id                                
                     left join tcs_latest_object_stats s
                            on s.id = o.id
                         where detection_list_id = %s
                           and followup_flag_date >= %s
                           and zooniverse_score > %s
                           and (processing_flags & %s = 0 or processing_flags is null)
                      order by followup_id
                    ) temp
                """, (listId, dateThreshold, rbThreshold, processingFlags))
            else:
                cursor.execute ("""
                    select id, followup_id, ifnull(ra_avg, ra) ra, ifnull(dec_avg, `dec`) `dec`, name, object_classification, detection_list_id, mjd from (
                        select o.id, followup_id, o.ra, o.`dec`, s.ra_avg, s.dec_avg, atlas_designation 'name', object_classification, detection_list_id, m.mjd
                          from atlas_diff_objects o
                          join atlas_detectionsddc d
                            on o.detection_id = d.id
                          join atlas_metadataddc m
                            on m.id = d.atlas_metadata_id
                     left join tcs_latest_object_stats s
                            on s.id = o.id
                         where detection_list_id = %s
                           and followup_flag_date >= %s
                           and (processing_flags & %s = 0 or processing_flags is null)
                     order by followup_id
                    ) temp
                """, (listId, dateThreshold, processingFlags))
            resultSet = cursor.fetchall ()
        else:
            cursor.execute ("""
                select id, followup_id, ifnull(ra_avg, ra) ra, ifnull(dec_avg, `dec`) `dec`, name, object_classification, detection_list_id, mjd from (
                    select o.id, followup_id, o.ra, o.`dec`, s.ra_avg, s.dec_avg, atlas_designation 'name', object_classification, detection_list_id, m.mjd
                      from atlas_diff_objects o
                      join atlas_detectionsddc d
                        on o.detection_id = d.id
                      join atlas_metadataddc m
                        on m.id = d.atlas_metadata_id
                 left join tcs_latest_object_stats s
                        on s.id = o.id
                     where o.id = %s
                ) temp
            """, (objectId,))
            resultSet = cursor.fetchone ()

        cursor.close ()

    except MySQLdb.Error as e:
        print("Error %d: %s" % (e.args[0], e.args[1]))
        sys.exit (1)

    return resultSet

def getAtlasObjectsByCustomList(conn, listId = 4):
    """getAtlasObjectsByCustomList.

    Args:
        conn:
        listId:
    """

    try:
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute ("""
            select id,
                   followup_id,
                   followup_flag_date,
                   ifnull(ra_avg, ra) ra,
                   ifnull(dec_avg, `dec`) `dec`,
                   name, object_classification,
                   detection_list_id,
                   mjd from (
                select o.id,
                       followup_id,
                       followup_flag_date,
                       ra,
                       `dec`,
                       s.ra_avg,
                       s.dec_avg,
                       atlas_designation 'name',
                       object_classification,
                       detection_list_id,
                       m.mjd
                  from atlas_diff_objects o
                  join atlas_metadataddc d
                    on o.detection_id = d.id
                  join tcs_object_groups g
                    on g.transient_object_id = o.id
                  join atlas_metadataddc m
                    on m.id = d.atlas_metadata_id
             left join tcs_latest_object_stats s
                    on s.id = o.id
                 where g.object_group_id = %s
              order by followup_id
            ) temp
            """, (listId,))
        resultSet = cursor.fetchall ()

        cursor.close ()

    except MySQLdb.Error as e:
        print("Error %d: %s" % (e.args[0], e.args[1]))
        sys.exit (1)

    return resultSet


def getPanSTARRSObjects(conn, listId = 4, dateThreshold = '2013-06-01', objectId = None, processingFlags = 0):
    """getPS1Objects.

    Args:
        conn:
        listId:
        dateThreshold:
        objectId:
    """

    try:
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)
        if objectId is None:
            cursor.execute ("""
                select o.id,
                       o.followup_id,
                       if(ra_psf<0, ra_psf + 360.0, ra_psf) 'ra',
                       dec_psf 'dec', o.ps1_designation 'name',
                       o.object_classification,
                       o.local_comments,
                       o.detection_list_id,
                       m.mjd_obs as mjd,
                       g.name as detection_stamp
                  from tcs_transient_objects o
                  join tcs_cmf_metadata m on o.tcs_cmf_metadata_id = m.id
             left join tcs_image_groups g on g.id = o.image_group_id
                 where o.detection_list_id = %s
                   and (o.observation_status is null or o.observation_status != 'mover')
                   and followup_flag_date >= %s
                   and (processing_flags & %s = 0 or processing_flags is null)
                 order by o.followup_id
            """, (listId, dateThreshold, processingFlags))
            resultSet = cursor.fetchall ()
        else:
            cursor.execute ("""
                select o.id,
                       followup_id,
                       if(ra_psf<0, ra_psf + 360.0, ra_psf) 'ra',
                       dec_psf 'dec',
                       o.ps1_designation 'name',
                       o.object_classification,
                       o.local_comments,
                       o.detection_list_id,
                       m.mjd_obs as mjd,
                       g.name as detection_stamp
                  from tcs_transient_objects o
                  join tcs_cmf_metadata m on o.tcs_cmf_metadata_id = m.id
             left join tcs_image_groups g on g.id = o.image_group_id
                 where o.id = %s
            """, (objectId,))
            resultSet = cursor.fetchone ()

        cursor.close ()

    except MySQLdb.Error as e:
        print("Error %d: %s" % (e.args[0], e.args[1]))
        sys.exit (1)

    return resultSet





def getATLASCandidates(conn, options, processingFlags = 0):
    candidateList = []

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
        rbThreshold = None
#        if options.rbthreshold is not None:
#           rbThreshold = float(options.rbthreshold)
        candidateList = getAtlasObjects(conn, listId = detectionList, dateThreshold = dateThreshold, rbThreshold = rbThreshold, processingFlags = processingFlags)

    return candidateList


# TODO: modify the Pan-STARRS code to get custom lists or re-use the ATLAS one.
def getPanSTARRSCandidates(conn, options, processingFlags = 0):
    candidateList = []

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
            dateThreshold = '2016-06-01'
    

    if len(options.candidate) > 0:
        for row in options.candidate:
            object = getPanSTARRSObjects(conn, objectId = int(row))
            if object:
                candidateList.append(object)

    else:
        # Get only the ATLAS objects that don't have the 'moons' flag set.
        candidateList = getPanSTARRSObjects(conn, listId = detectionList, dateThreshold = dateThreshold, processingFlags = processingFlags)
    
    return candidateList


# Update the processing flag AND change the observation_status.
def updateTransientObservationAndProcessingStatus(conn, objectId, processingFlag = PROCESSING_FLAGS['pmcheck'], observationStatus = None, survey = 'panstarrs'):

    rowsUpdated = 0
    
    try:
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)
        
        # 2012-02-29 KWS Make the query more efficient by only updating the relevant table

        # Try updating the tcs_transient_reobservations table

        if survey == 'panstarrs':
            cursor.execute ("""
                update tcs_transient_objects
                set observation_status = COALESCE(NULLIF(observation_status, ''), %s),
                    processing_flags = COALESCE(processing_flags, 0) | %s
                where id = %s
                """, (observationStatus, processingFlag, objectId))

        elif survey == 'atlas':
            cursor.execute ("""
                update atlas_diff_objects
                set observation_status = COALESCE(NULLIF(observation_status, ''), %s),
                    processing_flags = COALESCE(processing_flags, 0) | %s
                where id = %s
                """, (observationStatus, processingFlag, objectId))
    
        else:
            print("Need to specify survey as atlas or panstarrs.")
            cursor.close ()
            return rowsUpdated


        rowsUpdated = cursor.rowcount

        # Did we update any transient object rows? If not issue a warning.
        if rowsUpdated == 0:
            print("WARNING: No transient object entries were updated.")

        cursor.close ()


    except MySQLdb.Error as e:
        print(str(e))

    return rowsUpdated


# Insert an object comment. Note that this code, particularly the subquery
# may run into a COLLATION error. It's not obvious how to fix this.
def insertTransientObjectComment(conn, objectId, comment):

    import MySQLdb
    try:
        cursor = conn.cursor (MySQLdb.cursors.DictCursor)

        cursor.execute ("""
            INSERT INTO tcs_object_comments (transient_object_id, comment, date_inserted)
            SELECT %s, %s, NOW()
            WHERE NOT EXISTS (
                SELECT 1
                FROM tcs_object_comments
                WHERE transient_object_id = %s
                AND comment LIKE %s)
            """, (objectId, comment, objectId, comment))

    except MySQLdb.Error as e:
        print(str(e))

    cursor.close ()
    return conn.insert_id()

