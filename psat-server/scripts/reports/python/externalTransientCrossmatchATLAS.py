#!/usr/bin/env python
"""Refactored External Crossmatch code for ATLAS.

Usage:
  %s <configfile> [<candidate>...] [--list=<listid>] [--searchRadius=<radius>] [--update] [--date=<date>] [--ddc] [--updateSNType] [--tnsOnly] [--rbthreshold=<rbthreshold>]
  %s (-h | --help)
  %s --version

Options:
  -h --help                     Show this screen.
  --version                     Show version.
  --list=<listid>               The object list [default: 4]
  --customlist=<customlistid>   The object custom list
  --searchRadius=<radius>       Match radius (arcsec) [default: 3]
  --update                      Update the database
  --updateSNType                Update the Supernova Type in the objects table.
  --date=<date>                 Date threshold - no hyphens. If date is a small number assume number of days before NOW [default: 20160601]
  --ddc                         Use the ddc schema
  --tnsOnly                     Only search the TNS database.
  --rbthreshold=<rbthreshold>   Only check objects if they have a set RB Threshold (i.e. they have pixels).

  Example:
    %s ../../../../../atlas/config/config4_db5_readonly.yaml 1063629090302540900 --ddc --update --updateSNType
"""
import sys
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt
import os, MySQLdb, shutil, re
from gkutils.commonutils import find, Struct, cleanOptions, dbConnect, calculateRMSScatter, getAngularSeparation, coneSearchHTM, QUICK, FULL, CAT_ID_RA_DEC_COLS, PROCESSING_FLAGS
import MySQLdb
from datetime import datetime, timedelta


UNCLASSIFIED = 0
ORPHAN       = 1
VARIABLESTAR = 2
NT           = 4
AGN          = 8
SN           = 16
MISC         = 32
TDE          = 64
LENS         = 128
MOVER        = 256
BRIGHT       = 512

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


# PESSTO
#{'decDegErr': None,
#'hostRedshiftType': None,
#'decDeg': 18.9598,
#'targetImageUrl': 'http://nesssi.cacr.caltech.edu/catalina/20130513/jpg/1305131180844143958.master.jpg',
#'telescope': None,
#'lastNonDetectionDate': None,
#'discoveryPhase': None,
#'dateLastModified': datetime.datetime(2013, 5, 13, 17, 43, 37),
#'cy': -0.861820323173,
#'cx': -0.389490283791,
#'dateLastRead': datetime.datetime(2013, 5, 13, 17, 43, 37),
#'transientTypePredicationSource': None,
#'tripletImageUrl': None,
#'surveyObjectUrl': 'http://nesssi.cacr.caltech.edu/catalina/20130513/1305131180844143958.html',
#'masterIDFlag': 1L,
#'primaryKeyId': 36038L,
#'raDeg': 245.67993,
#'instrument': None,
#'hostRedshift': None,
#'xmz': None,
#'finderImageUrl': 'http://voeventnet.caltech.edu/feeds/ATEL/CRTS/1305131180844143958/1305131180844143958.find.jpg',
#'qubClassification': 16L,
#'subtractedImageUrl': None,
#'transientRedshift': None,
#'xmscale': None,
#'raDegErr': None,
#'dateCreated': datetime.datetime(2013, 5, 13, 17, 43, 37),
#'htm16ID': 56699337685L,
#'xmdistance': None,
#'observationDate': datetime.datetime(2013, 5, 13, 0, 0),
#'transientTypePrediction': 'SN',
#'observationMJD': 56425.0,
#'transientRedshiftNotes': None,
#'name': 'CSS130513-162243+185735',
#'transientBucketId': 36038L,
#'cz': 0.324904677403,
#'referenceImageUrl': None,
#'reducer': None,
#'xmdistanceModulus': None,
#'filter': 'R',
#'lastNonDetectionMJD': None,
#'magnitude': 16.55,
#'survey': 'crts-css',
#'magnitudeError': None,
#'htm20ID': 14515030447467L,
#'spectralType': None,
#'lightcurveURL': 'http://nesssi.cacr.caltech.edu/catalina/20130513/1305131180844143958p.html'}
#
# ATel
#{'singleClassification': None,
#'decDeg': 18.9597222222,
#'xmdistanceModulus': None,
#'cz': 0.324903393572,
#'cy': -0.861820917689,
#'cx': -0.389490039256,
#'titleToComment': 1,
#'atelNumber': 5061L,
#'primaryId': 3366L,
#'crossMatchDate': None,
#'raDeg': 245.679958333,
#'xmz': None,
#'ingested': 1L,
#'atelName': 'atel_5061',
#'atelUrl': 'http://www.astronomerstelegram.org/?read=5061',
#'supernovaTag': 1L,
#'summaryRow': 1,
#'xmdistance': None,
#'xmscale': None,
#'htm16ID': 56699337685L,
#'survey': 'atel-coords',
#'htm20ID': 14515030447590L}

# External crossmatch columns look like this:
# external_designation, type, host_galaxy, mag, discoverer, matched_list, other_info, separation, comments


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

def getAtlasObjects(conn, listId = 4, dateThreshold = '2016-01-01', objectId = None, rbThreshold = None):
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
                    select id, followup_id, ifnull(ra_avg, ra) ra, ifnull(dec_avg, `dec`) `dec`, name, object_classification, detection_list_id from (
                        select o.id, followup_id, ra, `dec`, s.ra_avg, s.dec_avg, atlas_designation 'name', object_classification, detection_list_id
                          from atlas_diff_objects o
                     left join tcs_latest_object_stats s
                            on s.id = o.id
                         where detection_list_id = %s
                           and followup_flag_date >= %s
                           and zooniverse_score > %s
                      order by followup_id
                    ) temp
                """, (listId, dateThreshold, rbThreshold))
            else:
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

def getAtlasObjectsByCustomList(conn, listId = 4):
    """getAtlasObjectsByCustomList.

    Args:
        conn:
        listId:
    """

    try:
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute ("""
            select id, followup_id, followup_flag_date, ifnull(ra_avg, ra) ra, ifnull(dec_avg, `dec`) `dec`, name, object_classification, detection_list_id from (
                select o.id, followup_id, followup_flag_date, ra, `dec`, s.ra_avg, s.dec_avg, atlas_designation 'name', object_classification, detection_list_id
                  from atlas_diff_objects o
                  join tcs_object_groups g
                    on g.transient_object_id = o.id
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


class SearchTNSONLY():
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

        print("\t* Crossmatching against TNS List")
        #searchDone, matches = searchCatalogue(connPESSTO, [objectRow], 'fs_tns_transients', radius = searchRadius)
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
        print(e)
#        if e[0] == 1142: # Can't delete - don't have permission
#            print("Can't delete.  User doesn't have permission.")
#        else:
#            print(e)

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
        print(e)
#        if e[0] == 1142: # Can't insert - don't have permission
#            print("Can't insert.  User doesn't have permission.")
#        else:
#            print(e)

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

        # 2012-02-29 KWS Make the query more efficient by only updating the relevant table

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

    # Use utils.Struct to convert the dict into an object for compatibility with old optparse code.
    options = Struct(**opts)

    import yaml
    with open(options.configfile) as yaml_file:
        config = yaml.load(yaml_file)

    username = config['databases']['local']['username']
    password = config['databases']['local']['password']
    database = config['databases']['local']['database']
    hostname = config['databases']['local']['hostname']

    candidateList = []

    catuser = config['databases']['catalogues']['username']
    catpass = config['databases']['catalogues']['password']
    catname = config['databases']['catalogues']['database']
    cathost = config['databases']['catalogues']['hostname']

    PESSTOCATUSER = config['databases']['pessto']['username']
    PESSTOCATPASS = config['databases']['pessto']['password']
    PESSTOCATNAME = config['databases']['pessto']['database']
    PESSTOCATHOST = config['databases']['pessto']['hostname']

    PESSTOCATPORT = 3306
    try:
        PESSTOCATPORT = config['databases']['pessto']['port']
    except KeyError as e:
        pass

    conn = dbConnect(hostname, username, password, database, quitOnError = True)

    # 2023-03-25 KWS MySQLdb disables autocommit by default. Switch it on globally.
    conn.autocommit(True)

    connCatalogues = dbConnect(cathost, catuser, catpass, catname, quitOnError = True)

    connPESSTO = dbConnect(PESSTOCATHOST, PESSTOCATUSER, PESSTOCATPASS, PESSTOCATNAME, lport=PESSTOCATPORT, quitOnError = False)


    detectionList = 4

    # If the list isn't specified assume it's the Eyeball List.
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
                dateThreshold = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        except:
            dateThreshold = '2016-06-01'

    if len(options.candidate) > 0:
        for row in options.candidate:
            object = getAtlasObjects(conn, objectId = int(row))
            if object:
                candidateList.append(object)

    else:
        # Get only the ATLAS objects that don't have the 'moons' flag set.
        candidateList = getAtlasObjects(conn, listId = detectionList, dateThreshold = dateThreshold)


    tables = getCatalogueTables(conn)

    results = crossmatchExternalLists(conn, connPESSTO, connCatalogues, candidateList, tables, searchRadius = float(options.searchRadius), tnsOnly = options.tnsOnly)

    if options.update and len(results) > 0:
        for result in results:
            deleteExternalCrossmatches(conn, result['id'])
            for matchrow in result['matches']:
                if not ((matchrow['matched_list'] == 'The PESSTO Transient Objects - primary object') and matchrow['ps1_designation'] == matchrow['external_designation']):
                    # Don't bother ingesting ourself from the PESSTO Marshall
                    print(matchrow['matched_list'], matchrow['ps1_designation'],  matchrow['external_designation'])
                    insertId = insertExternalCrossmatches(conn, matchrow)
                if options.updateSNType and matchrow['matched_list'] == 'Transient Name Server':
                    # Update the observation_status (spectral type) of the object according to
                    # what is in the TNS.
                    updateObjectSpecType(conn, result['id'], matchrow['type'])



    conn.close ()

    if connCatalogues:
        connCatalogues.close()

    if connPESSTO:
        connPESSTO.close()

    return




# ###########################################################################################
#                                         Main hook
# ###########################################################################################


if __name__=="__main__":
    main()

