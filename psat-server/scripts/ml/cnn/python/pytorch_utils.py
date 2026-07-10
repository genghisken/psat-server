import os

def getObjectsByList(conn, dbName, listId = 4, imageRoot='/db4/images/', ps1Data = False):
    # First get the candidates
    import MySQLdb
    try:
        cursor = conn.cursor (MySQLdb.cursors.DictCursor)

        if ps1Data:
            cursor.execute ("""
                select id
                  from tcs_transient_objects
                 where detection_list_id = %s
                   and confidence_factor is null
                   and tcs_images_id is not null
              order by followup_id desc
            """, (listId,))
        else:
            cursor.execute ("""
                select id
                  from atlas_diff_objects
                 where detection_list_id = %s
                   and zooniverse_score is null
            """, (listId,))
        resultSet = cursor.fetchall ()
        cursor.close ()

    except MySQLdb.Error as e:
        print("Error %d: %s" % (e.args[0], e.args[1]))

    return resultSet


def getImages(conn, dbName, objectList, imageRoot='/psdb3/images/', ps1Data = False):
    import MySQLdb
    images = []
    # Now, for each candidate, get the image
    for row in objectList:
        try:
            cursor = conn.cursor (MySQLdb.cursors.DictCursor)
            # 2022-01-01 KWS Use mjd_obs for Pan-STARRS but use filename to get MJD if ATLAS.
            #                Fixes issue with South Africa (and Chile) night number and MJD
            #                mismatch.
            if ps1Data:
                cursor.execute ("""
                select concat(%s ,%s,'/',truncate(mjd_obs,0), '/', image_filename,'.fits') as filename, filter from tcs_postage_stamp_images
                 where image_filename like concat(%s, '%%')
                   and image_filename not like concat(%s, '%%4300000000%%')
                   and image_type = 'diff'
                   and image_filename is not null
                   and pss_error_code = 0
                   and mjd_obs is not null
                """, (imageRoot, dbName, row['id'], row['id']))
            else:
                cursor.execute ("""
              -- 2025-12-31 KWS Emergency fix. will remove later.
              select concat(%s ,%s,'/',substr(image_filename,instr(image_filename, '_')+1,5), '/', image_filename,'.fits') as filename, ifnull(filter, '00000') as filter from tcs_postage_stamp_images
              -- select concat(s ,s,'/',if(instr(pss_filename,'skycell'),truncate(mjd_obs,0),substr(pss_filename,4,5)), '/', image_filename,'.fits') as filename, filter from tcs_postage_stamp_images
                 where image_filename like concat(%s, '%%')
                   and image_filename not like concat(%s, '%%4300000000%%')
                   and image_type = 'diff'
                   and image_filename is not null
                  --  and pss_error_code = 0
                  --  and mjd_obs is not null
                """, (imageRoot, dbName, row['id'], row['id']))

            imageResultSet = cursor.fetchall ()
            cursor.close ()
            for row in imageResultSet:
                # Only append images that actually exist!
                if os.path.exists(row['filename']):
                    print("Found: %s" % row['filename'])
                    images.append(row)
                else:
                    print("Can't find: %s" % row['filename'])


        except MySQLdb.Error as e:
            print("Error %d: %s" % (e.args[0], e.args[1]))

    return images

# Update the database.
def updateTransientRBValue(conn, objectId, realBogusValue, ps1Data = False):
    import MySQLdb

    rowsUpdated = 0

    try:
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)

        if ps1Data:
            # It's Pan-STARRS data
            cursor.execute ("""
                 update tcs_transient_objects
                 set confidence_factor = %s
                 where id = %s
            """, (realBogusValue, objectId))
        else:
            # It's ATLAS data
            cursor.execute ("""
                 update atlas_diff_objects
                 set zooniverse_score = %s
                 where id = %s
            """, (realBogusValue, objectId))

        rowsUpdated = cursor.rowcount

        # Did we update any transient object rows? If not issue a warning.
        if rowsUpdated == 0:
            print ("WARNING: No transient object entries were updated.")

        cursor.close ()


    except MySQLdb.Error as e:
        print ("Error %d: %s" % (e.args[0], e.args[1]))

    return rowsUpdated


def updateObjectRBFactors(conn, objectId, realBogusValue, tableName, columnName):

    import MySQLdb
    rowsUpdated = 0

    try:
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)

        statement = """
             update %s
             set %s = %s
             where id = %s
            -- and %s is null
        """ % (tableName, columnName, realBogusValue, objectId, columnName)
        cursor.execute(statement)

        rowsUpdated = cursor.rowcount

        # Did we update any transient object rows? If not issue a warning.
        if rowsUpdated == 0:
            print("WARNING: No transient object entries were updated.")

        cursor.close ()


    except MySQLdb.Error as e:
        print("Error %d: %s" % (e.args[0], e.args[1]))

    return rowsUpdated

