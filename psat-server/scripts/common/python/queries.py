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
        print(str(e))
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
        print(str(e))
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
        print(str(e))
        sys.exit (1)

    return resultSet





def getATLASCandidates(conn, options, processingFlags = 0):
    candidateList = []

    if options.list is not None:
        try:
            detectionList = int(options.list)
            if detectionList < 0 or detectionList > 13:
                print("Detection list must be between 0 and 13")
                return candidateList
        except ValueError as e:
            sys.exit("Detection list must be an integer")

    if options.customlist is not None:
        try:
            customlist = int(options.customlist)
            if customlist < 0 or customlist > 200:
                print("Custom list must be between 0 and 200")
                return candidateList
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
            if detectionList < 0 or detectionList > 13:
                print("Detection list must be between 0 and 13")
                return candidateList
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


def getAtlasObjectInfo(conn, objectId):
    """
    Get all object occurrences.
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
        print(str(e))
        sys.exit (1)

    return resultSet


# 2017-06-19 KWS Get object info as defined in DDC files.
# 2017-10-03 KWS Only pick up POSITIVE FLUX (det != 5).
# 2018-03-19 KWS Apparently, det != 5 is not quite enough to exclude negative flux!
# 2018-05-03 KWS Include an option to pick up the negative flux detections (e.g.
#                for calculating average RA and Dec). This will also get all the
#                deprecated detections.
def getAtlasObjectInfoddc(conn, objectId, negativeFlux = False):
    """
    Get all object occurrences.
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
        print(str(e))
        sys.exit (1)


    return resultSet


def getPSObjectInfo(conn, objectId):
    """
    Get all object occurrences. Grab the quality data as well so we can make a subsequent
    decision to reject the recurrence if necessary.
    """

    try:
        cursor = conn.cursor (MySQLdb.cursors.DictCursor)

        # Note that DEC is a MySQL reserved word, so need quotes around it

        # 2011-10-12 KWS Addded imageid to order by clause to make sure that the results
        #                are ordered consistently
        # 2013-08-26 KWS Addded flags so we can check for ghosts
        # 2021-10-02 KWS Addded flags2 so we can check for ghosts & crosstalk
        cursor.execute ("""
              SELECT d.ra_psf RA,
                     d.dec_psf 'DEC',
                     m.imageid,
                     substr(m.fpa_filter,1,1) Filter,
                     m.mjd_obs MJD,
                     m.filename Filename,
                     d.cal_psf_mag,
                     d.psf_inst_mag,
                     d.psf_inst_mag_sig,
                     d.ap_mag,
                     d.moments_xy,
                     d.flags,
                     d.flags2,
                     m.zero_pt,
                     m.exptime,
                     m.skycell,
                     m.tessellation field
              FROM tcs_transient_objects d, tcs_cmf_metadata m
              where d.id=%s
              and d.tcs_cmf_metadata_id = m.id
              UNION ALL
              SELECT d.ra_psf RA,
                     d.dec_psf 'DEC',
                     m.imageid,
                     substr(m.fpa_filter,1,1) Filter,
                     m.mjd_obs MJD,
                     m.filename Filename,
                     d.cal_psf_mag,
                     d.psf_inst_mag,
                     d.psf_inst_mag_sig,
                     d.ap_mag,
                     d.moments_xy,
                     d.flags,
                     d.flags2,
                     m.zero_pt,
                     m.exptime,
                     m.skycell,
                     m.tessellation field
              FROM tcs_transient_reobservations d, tcs_cmf_metadata m
              where d.transient_object_id=%s
              and d.tcs_cmf_metadata_id = m.id
              ORDER by MJD, imageid
        """, (objectId, objectId))
        result_set = cursor.fetchall ()

        cursor.close ()

    except MySQLdb.Error as e:
        print(str(e))
        sys.exit (1)


    return result_set


def getObjectInfo(conn, objectId, survey='atlas', ddc=True):
    objectInfo = None
    if survey == 'atlas':
        if ddc:
            objectInfo = getAtlasObjectInfoddc(conn, objectId)
        else:
            objectInfo = getAtlasObjectInfo(conn, objectId)
    elif survey == 'panstarrs':
        objectInfo = getPSObjectInfo(conn, objectId)
    return objectInfo



# 2013-10-11 KWS Rehash of LC detections query based on the Non-detections query below.
#                This allows us to request stamps based on new detections whilst not
#                overwriting the existing ones.
# 2015-05-31 KWS Added filename, ppsub_input, ppsub_reference to selection.
# 2021-12-28 KWS Pull out fpa_detector so we can identify which camera we need to make
#                requests for (GPC1 or GPC2).
LC_DET_QUERY = """\
         select mjd_obs mjd, substr(fpa_filter,1,1) filter, imageid, cast(truncate(mjd_obs,3) as char) tdate, ipp_idet, ra_psf, dec_psf, o.id, filename, ppsub_input, ppsub_reference, m.skycell sc, m.fpa_detector,
                case
                    when instr(m.filename,'MD') then substr(m.filename, instr(m.filename,'MD'),4)
                    when instr(m.filename,'RINGS') then substr(m.filename, instr(m.filename,'RINGS'),8)
                    else 'null'
                end as field,
                case
                    -- 3pi
                    when instr(m.filename,'WS') then if(instr(m.filename,'skycell'), replace(substr(m.filename, instr(m.filename,'skycell'),instr(m.filename,'.dif') - instr(m.filename,'skycell')), '.WS', ''), 'null')
                    -- MD
                    else if(instr(m.filename,'skycell'), replace(substr(m.filename, instr(m.filename,'skycell'),instr(m.filename,'.dif') - instr(m.filename,'skycell')), '.SS', ''), 'null')
                end as skycell
           from tcs_transient_objects o, tcs_cmf_metadata m
          where o.tcs_cmf_metadata_id = m.id
            and o.id = %s
            and o.cal_psf_mag is not null
            and (fpa_filter like concat(%s,'%%') or fpa_filter like concat(%s,'%%') or fpa_filter like concat(%s,'%%') or fpa_filter like concat(%s,'%%') or fpa_filter like concat(%s,'%%') or fpa_filter like concat(%s,'%%') or fpa_filter like concat(%s,'%%') or fpa_filter like concat(%s,'%%') or fpa_filter like concat(%s,'%%'))
         union all
         select mjd_obs mjd, substr(fpa_filter,1,1) filter, imageid, cast(truncate(mjd_obs,3) as char) tdate, ipp_idet, ra_psf, dec_psf, transient_object_id id, filename, ppsub_input, ppsub_reference, m.skycell sc, m.fpa_detector,
                case
                    when instr(m.filename,'MD') then substr(m.filename, instr(m.filename,'MD'),4)
                    when instr(m.filename,'RINGS') then substr(m.filename, instr(m.filename,'RINGS'),8)
                    else 'null'
                end as field,
                case
                    -- 3pi
                    when instr(m.filename,'WS') then if(instr(m.filename,'skycell'), replace(substr(m.filename, instr(m.filename,'skycell'),instr(m.filename,'.dif') - instr(m.filename,'skycell')), '.WS', ''), 'null')
                    -- MD
                    else if(instr(m.filename,'skycell'), replace(substr(m.filename, instr(m.filename,'skycell'),instr(m.filename,'.dif') - instr(m.filename,'skycell')), '.SS', ''), 'null')
                end as skycell
           from tcs_transient_reobservations r, tcs_cmf_metadata m
          where r.tcs_cmf_metadata_id = m.id
            and r.transient_object_id = %s
            and r.cal_psf_mag is not null
            and (fpa_filter like concat(%s,'%%') or fpa_filter like concat(%s,'%%') or fpa_filter like concat(%s,'%%') or fpa_filter like concat(%s,'%%') or fpa_filter like concat(%s,'%%') or fpa_filter like concat(%s,'%%') or fpa_filter like concat(%s,'%%') or fpa_filter like concat(%s,'%%') or fpa_filter like concat(%s,'%%'))
       order by mjd, imageid, ipp_idet
      """

# 2013-09-16 KWS Non-detections AND Blanks so that we can request postage stamps for blank areas...
# 2015-05-31 KWS Added filename, ppsub_input, ppsub_reference to selection.
# 2021-12-28 KWS Pull out fpa_detector so we can identify which camera we need to make
#                requests for (GPC1 or GPC2).
LC_NON_DET_AND_BLANKS_QUERY = """\
         select distinct mjd_obs mjd, substr(mm.fpa_filter,1,1) filter, imageid, cast(truncate(mm.mjd_obs,3) as char) tdate, field, det.skycell, filename, ppsub_input, ppsub_reference, mm.skycell sc, mm.zero_pt, mm.exptime, mm.deteff_counts, mm.deteff_magref, mm.deteff_calculated_offset, mm.fpa_detector
           from tcs_cmf_metadata mm,
           (
             select distinct field, skycell
               from (
               select
                      skycell,
                      case
                          when instr(m.tessellation,'MD') then substr(m.tessellation, instr(m.tessellation,'MD'),4)
                          when instr(m.tessellation,'RINGS') then substr(m.tessellation, instr(m.tessellation,'RINGS'),8)
                          else 'null'
                      end as field
                 from tcs_cmf_metadata m, tcs_transient_objects o
                where o.tcs_cmf_metadata_id = m.id
                  and o.id = %s
                union
               select
                      skycell,
                      case
                          when instr(m.tessellation,'MD') then substr(m.tessellation, instr(m.tessellation,'MD'),4)
                          when instr(m.tessellation,'RINGS') then substr(m.tessellation, instr(m.tessellation,'RINGS'),8)
                          else 'null'
                      end as field
                 from tcs_cmf_metadata m, tcs_transient_reobservations r
                where r.tcs_cmf_metadata_id = m.id
                  and r.transient_object_id = %s
               ) fieldandskycell
           ) det
         where mm.skycell = det.skycell and mm.tessellation like concat(det.field,'%%')
           and imageid not in
             (
                      select imageid
                        from (
                      select m.imageid
                        from tcs_transient_objects o, tcs_cmf_metadata m
                       where o.tcs_cmf_metadata_id = m.id
                         and o.id = %s
                         and o.cal_psf_mag is not null
                      union all
                      select m.imageid
                        from tcs_transient_reobservations r, tcs_cmf_metadata m
                       where r.tcs_cmf_metadata_id = m.id
                         and r.transient_object_id = %s
                         and r.cal_psf_mag is not null
                      ) temp2
             )
           and (fpa_filter like concat(%s,'%%') or fpa_filter like concat(%s,'%%') or fpa_filter like concat(%s,'%%') or fpa_filter like concat(%s,'%%') or fpa_filter like concat(%s,'%%') or fpa_filter like concat(%s,'%%') or fpa_filter like concat(%s,'%%') or fpa_filter like concat(%s,'%%') or fpa_filter like concat(%s,'%%'))
         order by mm.mjd_obs
      """

