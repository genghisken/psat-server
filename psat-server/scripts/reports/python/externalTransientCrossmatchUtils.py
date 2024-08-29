import MySQLdb
from datetime import datetime

UNCLASSIFIED = 0

from classifierSearchUtils import CLASSIFICATION_FLAGS, searchCatalogue


QUBLISTS = {
   0:"GARBAGE",
   1:"CONFIRMED",
   2:"GOOD",
   3:"POSSIBLE",
   4:"EYEBALL",
   5:"ATTIC",
   6:"ZOO",
   None:"UNLISTED"
}


class ExternalCrossmatches(object):
    """ExternalCrossmatches.
    """


    def __init__(self):
        """__init__.
        """
        self.externalCrossmatch = {}
        self.externalCrossmatch['external_designation'] = None
        self.externalCrossmatch['type'] = None
        self.externalCrossmatch['host_galaxy'] = None
        self.externalCrossmatch['mag'] = None
        self.externalCrossmatch['discoverer'] = None
        self.externalCrossmatch['other_info'] = None
        self.externalCrossmatch['separation'] = None
        self.externalCrossmatch['comments'] = None
        self.externalCrossmatch['url'] = None
        self.externalCrossmatch['host_z'] = None
        self.externalCrossmatch['object_z'] = None
        self.externalCrossmatch['disc_date'] = None
        self.externalCrossmatch['disc_filter'] = None

    def map(self, crossmatch):
        """map.

        Args:
            crossmatch:
        """
        return self.externalCrossmatch


class ExternalCrossmatches_view_transientBucketMaster(ExternalCrossmatches):
    """ExternalCrossmatches_view_transientBucketMaster.
    """


    def map(self, crossmatch):
        """map.

        Args:
            crossmatch:
        """
        self.externalCrossmatch['external_designation'] = crossmatch[1]['name']
        self.externalCrossmatch['type'] = crossmatch[1]['spectralType']
        self.externalCrossmatch['host_galaxy'] = None
        self.externalCrossmatch['mag'] = crossmatch[1]['magnitude']
        self.externalCrossmatch['discoverer'] = None
        self.externalCrossmatch['other_info'] = None
        self.externalCrossmatch['separation'] = crossmatch[0]
        self.externalCrossmatch['comments'] = None
        self.externalCrossmatch['url'] = crossmatch[1]['surveyObjectUrl']

        return self.externalCrossmatch

class ExternalCrossmatches_atel_coordinates(ExternalCrossmatches):
    """ExternalCrossmatches_atel_coordinates.
    """


    def map(self, crossmatch):
        """map.

        Args:
            crossmatch:
        """
        self.externalCrossmatch['external_designation'] = 'ATel ' + str(crossmatch[1]['atelNumber'])
        self.externalCrossmatch['other_info'] = crossmatch[1]['atelName']
        self.externalCrossmatch['separation'] = crossmatch[0]
        self.externalCrossmatch['url'] = "http://www.astronomerstelegram.org/?read=%s" % str(crossmatch[1]['atelNumber'])

        return self.externalCrossmatch

class ExternalCrossmatches_view_fs_crts_css_summary(ExternalCrossmatches):
    """ExternalCrossmatches_view_fs_crts_css_summary.
    """


    def map(self, crossmatch):
        """map.

        Args:
            crossmatch:
        """
        self.externalCrossmatch['external_designation'] = crossmatch[1]['name']
        self.externalCrossmatch['type'] = None
        self.externalCrossmatch['host_galaxy'] = None
        self.externalCrossmatch['mag'] = crossmatch[1]['mag']
        self.externalCrossmatch['discoverer'] = None
        self.externalCrossmatch['other_info'] = None
        self.externalCrossmatch['separation'] = crossmatch[0]
        try:
            self.externalCrossmatch['comments'] = crossmatch[1]['comment'].strip()
        except AttributeError as e:
            self.externalCrossmatch['comments'] = None
        self.externalCrossmatch['url'] = crossmatch[1]['surveyObjectUrl']

        return self.externalCrossmatch

class ExternalCrossmatches_view_fs_crts_mls_summary(ExternalCrossmatches):
    """ExternalCrossmatches_view_fs_crts_mls_summary.
    """


    def map(self, crossmatch):
        """map.

        Args:
            crossmatch:
        """
        self.externalCrossmatch['external_designation'] = crossmatch[1]['name']
        self.externalCrossmatch['type'] = None
        self.externalCrossmatch['host_galaxy'] = None
        self.externalCrossmatch['mag'] = crossmatch[1]['mag']
        self.externalCrossmatch['discoverer'] = None
        self.externalCrossmatch['other_info'] = None
        self.externalCrossmatch['separation'] = crossmatch[0]
        try:
            self.externalCrossmatch['comments'] = crossmatch[1]['comment'].strip()
        except AttributeError as e:
            self.externalCrossmatch['comments'] = None
        return self.externalCrossmatch
        self.externalCrossmatch['url'] = crossmatch[1]['surveyObjectUrl']

class ExternalCrossmatches_view_fs_crts_sss_summary(ExternalCrossmatches):
    """ExternalCrossmatches_view_fs_crts_sss_summary.
    """


    def map(self, crossmatch):
        """map.

        Args:
            crossmatch:
        """
        self.externalCrossmatch['external_designation'] = crossmatch[1]['name']
        self.externalCrossmatch['type'] = None
        self.externalCrossmatch['host_galaxy'] = None
        self.externalCrossmatch['mag'] = crossmatch[1]['mag']
        self.externalCrossmatch['discoverer'] = None
        self.externalCrossmatch['other_info'] = None
        self.externalCrossmatch['separation'] = crossmatch[0]
        try:
            self.externalCrossmatch['comments'] = crossmatch[1]['comment'].strip()
        except AttributeError as e:
            self.externalCrossmatch['comments'] = None
        return self.externalCrossmatch
        self.externalCrossmatch['url'] = crossmatch[1]['surveyObjectUrl']

class ExternalCrossmatches_view_cbats_sn(ExternalCrossmatches):
    """ExternalCrossmatches_view_cbats_sn.
    """


    def map(self, crossmatch):
        """map.

        Args:
            crossmatch:
        """
        self.externalCrossmatch['external_designation'] = crossmatch[1]['name']
        self.externalCrossmatch['type'] = crossmatch[1]['snType']
        self.externalCrossmatch['host_galaxy'] = crossmatch[1]['hostGalaxy']
        self.externalCrossmatch['mag'] = crossmatch[1]['mag']
        self.externalCrossmatch['discoverer'] = crossmatch[1]['discoverers']
        self.externalCrossmatch['other_info'] = crossmatch[1]['discoveryRef']
        self.externalCrossmatch['separation'] = crossmatch[0]
        self.externalCrossmatch['url'] = "http://www.cbat.eps.harvard.edu/lists/Supernovae.html"

        return self.externalCrossmatch


class ExternalCrossmatches_view_cbats_psn(ExternalCrossmatches):
    """ExternalCrossmatches_view_cbats_psn.
    """


    def map(self, crossmatch):
        """map.

        Args:
            crossmatch:
        """
        self.externalCrossmatch['external_designation'] = crossmatch[1]['name']
        self.externalCrossmatch['type'] = crossmatch[1]['snType']
        self.externalCrossmatch['host_galaxy'] = crossmatch[1]['hostGalaxy']
        self.externalCrossmatch['mag'] = crossmatch[1]['mag']
        self.externalCrossmatch['discoverer'] = crossmatch[1]['discoverers']
        self.externalCrossmatch['other_info'] = crossmatch[1]['discoveryRef']
        self.externalCrossmatch['separation'] = crossmatch[0]
        self.externalCrossmatch['url'] = "http://www.cbat.eps.harvard.edu/unconf/tocp.html"

        return self.externalCrossmatch

# 2015-04-21 KWS Added ASASSN SNe and Transient Crossmatches

class ExternalCrossmatches_fs_asassn_sne(ExternalCrossmatches):
    """ExternalCrossmatches_fs_asassn_sne.
    """


    def map(self, crossmatch):
        """map.

        Args:
            crossmatch:
        """
        self.externalCrossmatch['external_designation'] = crossmatch[1]['ID']
        self.externalCrossmatch['type'] = crossmatch[1]['Type']
        self.externalCrossmatch['host_galaxy'] = crossmatch[1]['Galaxy_name']
        self.externalCrossmatch['mag'] = crossmatch[1]['V_disc']
        self.externalCrossmatch['discoverer'] = None
        self.externalCrossmatch['other_info'] = 'Disc date: ' + str(crossmatch[1]['Date'])
        self.externalCrossmatch['separation'] = crossmatch[0]
        self.externalCrossmatch['comments'] = 'z=' + str(crossmatch[1]['Redshift'])
        self.externalCrossmatch['url'] = crossmatch[1]['surveyUrl']

        return self.externalCrossmatch


class ExternalCrossmatches_fs_asassn_transients(ExternalCrossmatches):
    """ExternalCrossmatches_fs_asassn_transients.
    """


    def map(self, crossmatch):
        """map.

        Args:
            crossmatch:
        """
        self.externalCrossmatch['external_designation'] = crossmatch[1]['name']
        self.externalCrossmatch['type'] = None
        self.externalCrossmatch['host_galaxy'] = None
        self.externalCrossmatch['mag'] = crossmatch[1]['Vmag']
        self.externalCrossmatch['discoverer'] = None
        self.externalCrossmatch['other_info'] = 'Disc date: ' + str(crossmatch[1]['discDate'])
        self.externalCrossmatch['separation'] = crossmatch[0]
        self.externalCrossmatch['comments'] = crossmatch[1]['comment']
        self.externalCrossmatch['url'] = crossmatch[1]['surveyUrl']

        return self.externalCrossmatch


class ExternalCrossmatches_fs_tns_transients(ExternalCrossmatches):
    """ExternalCrossmatches_fs_tns_transients.
    """


    def map(self, crossmatch):
        """map.

        Args:
            crossmatch:
        """
        self.externalCrossmatch['external_designation'] = crossmatch[1]['objectName']
        self.externalCrossmatch['type'] = None
        self.externalCrossmatch['host_galaxy'] = crossmatch[1]['hostName']
        self.externalCrossmatch['mag'] = crossmatch[1]['discMag']
        self.externalCrossmatch['discoverer'] = crossmatch[1]['discoverer']
        if crossmatch[1]['discDate']:
            self.externalCrossmatch['other_info'] = 'Disc date: ' + crossmatch[1]['discDate'].strftime("%Y-%m-%d %H:%M:%S")
        else:
            self.externalCrossmatch['other_info'] = None

        self.externalCrossmatch['separation'] = crossmatch[0]
        comments = ''
        if crossmatch[1]['hostRedshift']:
            comments += 'host z=' + str(crossmatch[1]['hostRedshift'])
        if crossmatch[1]['transRedshift']:
            comments += ' object z=' + str(crossmatch[1]['transRedshift'])

        if comments:
            self.externalCrossmatch['comments'] = comments.strip()
        else:
            self.externalCrossmatch['comments'] = None
        self.externalCrossmatch['url'] = crossmatch[1]['objectUrl']

        return self.externalCrossmatch

# 2019-10-07 KWS New version of TNS

class ExternalCrossmatches_tcs_cat_tns(ExternalCrossmatches):
    """ExternalCrossmatches_tcs_cat_tns.
    """


    def map(self, crossmatch):
        """map.

        Args:
            crossmatch:
        """
        self.externalCrossmatch['external_designation'] = crossmatch[1]['tns_name']

        if crossmatch[1]['type'] and crossmatch[1]['type'] != 'null':
            self.externalCrossmatch['type'] = crossmatch[1]['type']
        self.externalCrossmatch['host_galaxy'] = crossmatch[1]['host_name']
        self.externalCrossmatch['mag'] = crossmatch[1]['disc_mag']
        if crossmatch[1]['sender']:
            self.externalCrossmatch['discoverer'] = crossmatch[1]['sender'].replace('_Bot1','').replace('_Bot','').replace('_bot','')
        if crossmatch[1]['disc_int_name']:
            self.externalCrossmatch['other_info'] = crossmatch[1]['disc_int_name']
        else:
            self.externalCrossmatch['other_info'] = None

        self.externalCrossmatch['separation'] = crossmatch[0]

        # Create a comment for the discovery.
        #comments = None
        #commentList = []

        #if commentList:
        #    comments = ', '.join(commentList)

        #if comments:
        #    self.externalCrossmatch['comments'] = comments.strip()
        #else:
        #    self.externalCrossmatch['comments'] = None

        self.externalCrossmatch['url'] = 'https://wis-tns.org/object/' + crossmatch[1]['tns_name']

        if crossmatch[1]['hostz']:
            self.externalCrossmatch['host_z'] = crossmatch[1]['hostz']
        if crossmatch[1]['z']:
            self.externalCrossmatch['object_z'] = crossmatch[1]['z']
        if crossmatch[1]['disc_date']:
            self.externalCrossmatch['disc_date'] = crossmatch[1]['disc_date'].strftime("%Y-%m-%d %H:%M:%S")
        if crossmatch[1]['disc_mag_filter'] and crossmatch[1]['disc_mag']:
            self.externalCrossmatch['disc_filter'] = crossmatch[1]['disc_mag_filter']

        return self.externalCrossmatch


class ExternalCrossmatchesFactory(object):
    """ExternalCrossmatchesFactory.
    """


    def getExternalCrossmatch(self, tableName):
        """getExternalCrossmatch.

        Args:
            tableName:
        """
        className = 'ExternalCrossmatches_' + tableName
        return eval(className)()


# 2016-02-15 KWS Altered the query to extract the AVERAGE ra and dec if they
#                exist, OR the first detection ra and dec if they don't.

def getAtlasObjects(conn, listId = 4, dateThreshold = '2016-01-01', objectId = None):
    """getAtlasObjects.

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
                select id, followup_id, ifnull(ra_avg, ra) ra, ifnull(dec_avg, `dec`) `dec`, name, object_classification, detection_list_id from (
                    select o.id, followup_id, ra, `dec`, s.ra_avg, s.dec_avg, atlas_designation 'name', object_classification, detection_list_id
                      from atlas_diff_objects o
                 left join tcs_latest_object_stats s
                        on s.id = o.id
                     where detection_list_id = %s
                       and followup_flag_date >= %s
                  order by followup_id
                ) temp
            """, (listId, dateThreshold))
            resultSet = cursor.fetchall ()
        else:
            cursor.execute ("""
                select id, followup_id, ifnull(ra_avg, ra) ra, ifnull(dec_avg, `dec`) `dec`, name, object_classification, detection_list_id from (
                    select o.id, followup_id, ra, `dec`, s.ra_avg, s.dec_avg, atlas_designation 'name', object_classification, detection_list_id
                      from atlas_diff_objects o
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


def getPS1Objects(conn, listId = 4, dateThreshold = '2013-06-01', objectId = None):
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
                select id, followup_id, if(ra_psf<0, ra_psf + 360.0, ra_psf) 'ra', dec_psf 'dec', ps1_designation 'name', object_classification, local_comments, detection_list_id
                  from tcs_transient_objects
                 where detection_list_id = %s
                   and (observation_status is null or observation_status != 'mover')
                   and followup_flag_date >= %s
                 order by followup_id
            """, (listId, dateThreshold))
            resultSet = cursor.fetchall ()
        else:
            cursor.execute ("""
                select id, followup_id, if(ra_psf<0, ra_psf + 360.0, ra_psf) 'ra', dec_psf 'dec', ps1_designation 'name', object_classification, local_comments, detection_list_id
                  from tcs_transient_objects
                 where id = %s
            """, (objectId,))
            resultSet = cursor.fetchone ()

        cursor.close ()

    except MySQLdb.Error as e:
        print("Error %d: %s" % (e.args[0], e.args[1]))
        sys.exit (1)

    return resultSet


def getCatalogueTables(conn):
    """getCatalogueTables.

    Args:
        conn:
    """

    try:
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute ("""
            select id, table_name, description, url
            from tcs_catalogue_tables
        """)

        resultSet = cursor.fetchall ()
        cursor.close ()

    except MySQLdb.Error as e:
        print("Error %d: %s" % (e.args[0], e.args[1]))
        sys.exit (1)

    # Build up the dictionary of table names vs descriptions
    tables = {}
    for row in resultSet:
        tables[row['table_name']] = row['description']

    return tables


class SearchEXTERNALTRANSIENTS():
    """
        External Transient List Search Algorithm.

        Eliminate objects that are bright nearby stars or galaxies within a specified radius
        e.g 15 arcsec.
    """

    def __init__(self, searchRadius):
        """__init__.

        Args:
            searchRadius:
        """
        self.searchRadius = searchRadius


    def searchField(self, connPESSTO, connCatalogues, objectRow):
        """
        Find matches for this transient in the CBAT, ATel, CRTS and PESSTO lists
        """


        objectType = UNCLASSIFIED
        print("\tRunning External Transient List Search Algorithm")

        searchRadius = self.searchRadius

        allMatches = {}
        searchDone = False

        #if not match:
        # 2015-06-10 KWS For ATels we want EVERY match, not just the nearest.
        print("\t* Crossmatching against ATel List")
        searchDone, matches = searchCatalogue(connPESSTO, [objectRow], 'atel_coordinates', radius = searchRadius)
        if searchDone and matches:
            m = []
            for match in matches[0][1]:
                separation = match[0]
                matchRow = match[1]
                m.append([separation, matchRow])
            table = matches[0][2]
            allMatches[table] = m

        #if not match:
        print("\t* Crossmatching against CSS List")
        searchDone, matches = searchCatalogue(connPESSTO, [objectRow], 'view_fs_crts_css_summary', radius = searchRadius)
        if searchDone and matches:
            separation = matches[0][1][0][0]
            nearestMatch = matches[0][1][0][1]
            table = matches[0][2]
            allMatches[table] = [[separation, nearestMatch]]

        #if not match:
        print("\t* Crossmatching against MLS List")
        searchDone, matches = searchCatalogue(connPESSTO, [objectRow], 'view_fs_crts_mls_summary', radius = searchRadius)
        if searchDone and matches:
            separation = matches[0][1][0][0]
            nearestMatch = matches[0][1][0][1]
            table = matches[0][2]
            allMatches[table] = [[separation, nearestMatch]]

        #if not match:
        print("\t* Crossmatching against SSS List")
        searchDone, matches = searchCatalogue(connPESSTO, [objectRow], 'view_fs_crts_sss_summary', radius = searchRadius)
        if searchDone and matches:
            separation = matches[0][1][0][0]
            nearestMatch = matches[0][1][0][1]
            table = matches[0][2]
            allMatches[table] = [[separation, nearestMatch]]


        # 2015-04-21 KWS Added ASASSN crossmatches

        #if not match:
        print("\t* Crossmatching against ASASSN SNe List")
        searchDone, matches = searchCatalogue(connPESSTO, [objectRow], 'fs_asassn_sne', radius = searchRadius)
        if searchDone and matches:
            separation = matches[0][1][0][0]
            nearestMatch = matches[0][1][0][1]
            table = matches[0][2]
            allMatches[table] = [[separation, nearestMatch]]

        #if not match:
        print("\t* Crossmatching against ASASSN Transients List")
        searchDone, matches = searchCatalogue(connPESSTO, [objectRow], 'fs_asassn_transients', radius = searchRadius)
        if searchDone and matches:
            separation = matches[0][1][0][0]
            nearestMatch = matches[0][1][0][1]
            table = matches[0][2]
            allMatches[table] = [[separation, nearestMatch]]

        print("\t* Crossmatching against TNS List")
        #searchDone, matches = searchCatalogue(connPESSTO, [objectRow], 'fs_tns_transients', radius = searchRadius)
        searchDone, matches = searchCatalogue(connCatalogues, [objectRow], 'tcs_cat_tns', radius = searchRadius)
        if searchDone and matches:
            separation = matches[0][1][0][0]
            nearestMatch = matches[0][1][0][1]
            table = matches[0][2]
            allMatches[table] = [[separation, nearestMatch]]

        #if not match:
        print("\t* Crossmatching against PESSTO List")
        searchDone, matches = searchCatalogue(connPESSTO, [objectRow], 'view_transientBucketMaster', radius = searchRadius)
        if searchDone and matches:
            separation = matches[0][1][0][0]
            nearestMatch = matches[0][1][0][1]
            table = matches[0][2]
            allMatches[table] = [[separation, nearestMatch]]


        return allMatches

# 2020-03-31 KWS Added TNS only algorithm for fast testing against very large lists.
class SearchTNSONLY():
    """
        External Transient List Search Algorithm. Just search the TNS.

        Eliminate objects that are bright nearby stars or galaxies within a specified radius
        e.g 15 arcsec.
    """

    def __init__(self, searchRadius):
        """__init__.

        Args:
            searchRadius:
        """
        self.searchRadius = searchRadius


    def searchField(self, connPESSTO, connCatalogues, objectRow):
        """
        Find matches for this transient in the CBAT, ATel, CRTS and PESSTO lists
        """


        objectType = UNCLASSIFIED
        print("\tRunning External Transient List Search Algorithm")

        searchRadius = self.searchRadius

        allMatches = {}
        searchDone = False

        print("\t* Crossmatching against TNS List")
        searchDone, matches = searchCatalogue(connCatalogues, [objectRow], 'tcs_cat_tns', radius = searchRadius)
        if searchDone and matches:
            separation = matches[0][1][0][0]
            nearestMatch = matches[0][1][0][1]
            table = matches[0][2]
            allMatches[table] = [[separation, nearestMatch]]

        return allMatches


def deleteExternalCrossmatches(conn, objectId):
    """deleteExternalCrossmatches.

    Args:
        conn:
        objectId:
    """
    print(objectId)
    try:
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute ("""
            delete from tcs_cross_matches_external
            where transient_object_id = %s
            """, (objectId,))

        cursor.close ()

    except MySQLdb.Error as e:
        if e[0] == 1142: # Can't delete - don't have permission
            print("Can't delete.  User doesn't have permission.")
        else:
            print(e)

    cursor.close ()
    return conn.affected_rows()


def insertExternalCrossmatches(conn, matchRow):
    """insertExternalCrossmatches.

    Args:
        conn:
        matchRow:
    """

    try:
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)

        cursor.execute ("""
            insert into tcs_cross_matches_external (
               transient_object_id,
               external_designation,
               type,
               host_galaxy,
               mag,
               discoverer,
               matched_list,
               other_info,
               separation,
               comments,
               url,
               host_z,
               object_z,
               disc_date,
               disc_filter)
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (matchRow['transient_object_id'],
                  matchRow['external_designation'],
                  matchRow['type'],
                  matchRow['host_galaxy'],
                  matchRow['mag'],
                  matchRow['discoverer'],
                  matchRow['matched_list'],
                  matchRow['other_info'],
                  matchRow['separation'],
                  matchRow['comments'],
                  matchRow['url'],
                  matchRow['host_z'],
                  matchRow['object_z'],
                  matchRow['disc_date'],
                  matchRow['disc_filter']))

        cursor.close ()

    except MySQLdb.Error as e:
        if e[0] == 1142: # Can't insert - don't have permission
            print("Can't insert.  User doesn't have permission.")
        else:
            print(e)

    cursor.close ()
    return conn.insert_id()

# Update the spectral type ONLY if it's not currently set.
# We can query for differences later.
def updateObjectSpecType(conn, objectId, specType):
    """updateObjectSpecType.

    Args:
        conn:
        objectId:
        specType:
    """

    rowsUpdated = 0

    try:
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)

        # Try updating the tcs_transient_reobservations table
        cursor.execute ("""
             update atlas_diff_objects
             set observation_status = %s
             where id = %s
             and (observation_status is null or observation_status = '')
        """, (specType, objectId))

        rowsUpdated = cursor.rowcount

        # Did we update any transient object rows? If not issue a warning.
        if rowsUpdated == 0:
            print("WARNING: No transient object entries were updated.")

        cursor.close ()


    except MySQLdb.Error as e:
        print("Error %d: %s" % (e.args[0], e.args[1]))

    return rowsUpdated


def updateObjectSpecTypePS1(conn, objectId, specType):
    """updateObjectSpecTypePS1.

    Args:
        conn:
        objectId:
        specType:
    """

    rowsUpdated = 0

    try:
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)

        # Try updating the tcs_transient_reobservations table
        cursor.execute ("""
             update tcs_transient_objects
             set observation_status = %s
             where id = %s
             and (observation_status is null or observation_status = '')
        """, (specType, objectId))

        rowsUpdated = cursor.rowcount

        # Did we update any transient object rows? If not issue a warning.
        if rowsUpdated == 0:
            print("WARNING: No transient object entries were updated.")

        cursor.close ()


    except MySQLdb.Error as e:
        print("Error %d: %s" % (e.args[0], e.args[1]))

    return rowsUpdated



def crossmatchExternalLists(conn, connPESSTO, connCatalogues, transientList, tables, searchRadius = 3.0, tnsOnly = False):
    """crossmatchExternalLists.

    Args:
        conn:
        connPESSTO:
        connCatalogues:
        transientList:
        tables:
        searchRadius:
        tnsOnly:
    """

    results = []

    if tnsOnly:
        objectClassifier = SearchTNSONLY(searchRadius)
    else:
        objectClassifier = SearchEXTERNALTRANSIENTS(searchRadius)

    classifications = 1
    listLength = len(transientList)
    for row in transientList:
        print("Crossmatching object", row['id'], row['name'], classifications, '/', listLength)
        matches = objectClassifier.searchField(connPESSTO, connCatalogues, row)

        matchedItems = []

        if matches:
            for table, matchValues in matches.items():
               # 2015-06-10 KWS We don't always JUST want to record the nearest value.
               #                E.g. with ATels, we want ALL of the matching values.

               for value in matchValues:
                   matchRow = {}
                   matchRow['transient_object_id'] = row['id']
                   matchRow['local_designation'] = row['name'] 
                   matchRow['ps1_designation'] = row['name']
                   matchRow['detection_list_id'] = row['detection_list_id'] 
                   matchRow['ra'] = row['ra']
                   matchRow['dec'] = row['dec']
                   try:
                       matchRow['matched_list'] = tables[table]
                   except KeyError as e:
                       matchRow['matched_list'] = table

                   f = ExternalCrossmatchesFactory()
                   a = f.getExternalCrossmatch(table)

                   externalCrossmatchRow = a.map(value)

                   matchRow['external_designation'] = externalCrossmatchRow['external_designation']
                   matchRow['type'] = externalCrossmatchRow['type']
                   matchRow['host_galaxy'] = externalCrossmatchRow['host_galaxy']
                   matchRow['mag'] = externalCrossmatchRow['mag']
                   matchRow['discoverer'] = externalCrossmatchRow['discoverer']
                   matchRow['other_info'] = externalCrossmatchRow['other_info']
                   matchRow['separation'] = externalCrossmatchRow['separation']
                   matchRow['comments'] = externalCrossmatchRow['comments']
                   matchRow['url'] = externalCrossmatchRow['url']
                   matchRow['host_z'] = externalCrossmatchRow['host_z']
                   matchRow['object_z'] = externalCrossmatchRow['object_z']
                   matchRow['disc_date'] = externalCrossmatchRow['disc_date']
                   matchRow['disc_filter'] = externalCrossmatchRow['disc_filter']

                   matchedItems.append(matchRow)

            results.append({'id': row['id'], 'matches': matchedItems})
        classifications += 1

    return results

