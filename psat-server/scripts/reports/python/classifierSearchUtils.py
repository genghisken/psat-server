import MySQLdb
#from utils import *
from gkutils.commonutils import enum, coneSearchHTM, QUICK, FULL, CAT_ID_RA_DEC_COLS
import math

ALGORITHMS = enum(THREEPI='THREEPI', MD01='MD01', MD02='MD02', MD03='MD03', MD04='MD04', MD05='MD05', MD06='MD06', MD07='MD07', MD08='MD08', MD09='MD09', MD10='MD10', PESSTO='PESSTO', NEARBYBRIGHT='NEARBYBRIGHT', KEPLERGALAXIES='KEPLERGALAXIES', ATLAS='ATLAS')

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
KEPLER       = 1024

CLASSIFICATION_FLAGS = {'unclassified':   UNCLASSIFIED,
                        'orphan':         ORPHAN,
                        'variablestar':   VARIABLESTAR,
                        'nt':             NT,
                        'agn':            AGN,
                        'sn':             SN,
                        'miscellaneous':  MISC,
                        'tde':            TDE,
                        'lens':           LENS,
                        'mover':          MOVER,
                        'bright':         BRIGHT,
                        'kepler':         KEPLER}


# Do a cone search of the named catalogue


def searchCatalogue(conn, objectList, catalogueName, radius = 3.0):
   """Cone Search wrapper to make it a little more user friendly"""

   # objectList MUST be a list or tuple of dicts.  The minumum dict values it should contain are:
   # "ra"
   # "dec"
   # "id"
   # "name" (equal to "id" unless it has a more human readable name)

   matchedObjects = []
   searchDone = True

   for row in objectList:
      #message, xmObjects = coneSearch(row['ra'], row['dec'], radius, catalogueName, queryType = FULL, conn = conn)
      message, xmObjects = coneSearchHTM(row['ra'], row['dec'], radius, catalogueName, queryType = FULL, conn = conn)

      # Did we search the catalogues correctly?
      if message and (message.startswith('Error') or 'not recognised' in message):
         # Successful cone searches should not return an error message, otherwise something went wrong.
         print("Database error - cone search unsuccessful.  Message was:")
         print("\t%s" % message)
         searchDone = False

      if xmObjects:
         numberOfMatches = len(xmObjects)
         nearestSep = xmObjects[0][0]
         nearestCatRow = xmObjects[0][1]
         nearestCatId = nearestCatRow[CAT_ID_RA_DEC_COLS[catalogueName][0][0]]
         #print row['id'], row['name'], nearestCatId, nearestSep, numberOfMatches

         redshift = None
         xmz = None
         xmscale = None
         xmdistance = None
         xmdistanceModulus = None

         for xm in xmObjects:
            # If there's a redshift, calculate physical parameters

            if len(CAT_ID_RA_DEC_COLS[catalogueName][0]) > 3:
               # The catalogue has a redshift column
               redshift = xm[1][CAT_ID_RA_DEC_COLS[catalogueName][0][3]]

            if redshift and redshift > 0.0:
               # Calculate distance modulus, etc
               redshiftInfo = redshiftToDistance(redshift)

               if redshiftInfo:
                  xmz = redshiftInfo['z']
                  xmscale = redshiftInfo['da_scale']
                  xmdistance = redshiftInfo['dl_mpc']
                  xmdistanceModulus = redshiftInfo['dmod']

            # Add the calculated parameters to the crossmatch dictionary.  This
            # assumes that there are no columns called xmz, xmscale, xmdistance,
            # xmdistanceModulus. Modify the crossmatch row in situ.

            xm[1]['xmz'] = xmz
            xm[1]['xmscale'] = xmscale
            xm[1]['xmdistance'] = xmdistance
            xm[1]['xmdistanceModulus'] = xmdistanceModulus

         matchedObjects.append([row, xmObjects, catalogueName])

         # Explanation:
         #    matchedObjects[0] = original input row
         #    matchedObjects[1] = the crossmatches
         #       This is a list of tuples that contains (separation, results)
         #       so the [0] = separation, [1] = results row
         #       that contains [separation, catalogueRow]
         #    matchedObjects[2] = the catalogue we searched

   return searchDone, matchedObjects


# Why include the complexity of having a separate method for each catalogue?
# Because a successful cone search doesn't necessarily mean a successful match.

class CatalogueCrossmatchUtils:
   """Similar to the C++ crossmatch utils library"""

   def crossmatchWithNEDGalaxies(self, conn, objectRow, radius = 3.0, physicalRadius = 0.5, angularOnly = False):
      """crossmatchWithNEDGalaxies.

      Args:
          conn:
          objectRow:
          radius:
          physicalRadius:
          angularOnly:
      """

      # NED Physical Galaxy search
      searchDone, matches = searchCatalogue(conn, [objectRow], 'tcs_cat_v_ned_galaxies', radius = radius)

      if angularOnly:
         return searchDone, matches

      # OK - we have some angular separation matches. Now search through these for matches with
      # a physical separation within the physical radius.

      matchedObjects = []
      matchSubset = []
      if searchDone and matches:
         for row in matches[0][1]:
            if row[1]["xmscale"] and row[1]["xmscale"] * row[0] < physicalRadius:
               print("\t\tPhysical separation = %.2f kpc" % (row[1]["xmscale"] * row[0]))
               matchSubset.append(row)

      if matchSubset:
         matchedObjects.append([matches[0][0], matchSubset, matches[0][2]])

      return searchDone, matchedObjects


   def crossmatchWithSDSSSpecGalaxies(self, conn, objectRow, radius = 3.0, physicalRadius = 0.5):
      """crossmatchWithSDSSSpecGalaxies.

      Args:
          conn:
          objectRow:
          radius:
          physicalRadius:
      """

      # SDSS Spectroscopic Galaxy search
      searchDone, matches = searchCatalogue(conn, [objectRow], 'tcs_sdss_spect_galaxies_cat', radius = radius)

      # OK - we have some angular separation matches. Now search through these for matches with
      # a physical separation within the physical radius.

      matchedObjects = []
      matchSubset = []
      if searchDone and matches:
         for row in matches[0][1]:
            if row[1]["xmscale"] and row[1]["xmscale"] * row[0] < physicalRadius:
               print("\t\tPhysical separation = %.2f kpc" % (row[1]["xmscale"] * row[0]))
               matchSubset.append(row)


      if matchSubset:
         matchedObjects.append([matches[0][0], matchSubset, matches[0][2]])

      return searchDone, matchedObjects


   # 2015-02-13 KWS I made a mistake calculating the physical separation. xmscale x angular sep should be used.
   def crossmatchWithSDSSDR9SpecGalaxies(self, conn, objectRow, radius = 3.0, physicalRadius = 0.5):
      """crossmatchWithSDSSDR9SpecGalaxies.

      Args:
          conn:
          objectRow:
          radius:
          physicalRadius:
      """

      # SDSS Spectroscopic Galaxy search
      searchDone, matches = searchCatalogue(conn, [objectRow], 'tcs_cat_v_sdss_dr9_spect_galaxies', radius = radius)

      # OK - we have some angular separation matches. Now search through these for matches with
      # a physical separation within the physical radius.

      matchedObjects = []
      matchSubset = []
      if searchDone and matches:
         for row in matches[0][1]:
            if row[1]["xmscale"] and row[1]["xmscale"] * row[0] < physicalRadius:
               print("\t\tPhysical separation = %.2f kpc" % (row[1]["xmscale"] * row[0]))
               matchSubset.append(row)


      if matchSubset:
         matchedObjects.append([matches[0][0], matchSubset, matches[0][2]])

      return searchDone, matchedObjects


   def crossmatchWithFaintSDSSPhotoStars(self, conn, objectRow, radius = 3.4):
      """crossmatchWithFaintSDSSPhotoStars.

      Args:
          conn:
          objectRow:
          radius:
      """

      searchDone, matches = searchCatalogue(conn, [objectRow], 'tcs_sdss_stars_cat', radius = radius)

      matchedObjects = []
      matchSubset = []
      if searchDone and matches:
         for row in matches[0][1]:
            #uMag = row[1]["petroMag_u"]
            #gMag = row[1]["petroMag_g"]
            rMag = row[1]["petroMag_r"]
            #iMag = row[1]["petroMag_i"]
            #zMag = row[1]["petroMag_z"]

            #if uMag > 21.0 and gMag > 21.0 and rMag > 21.0 and iMag > 21.0 and zMag > 21.0:
            if rMag > 21.0:
               matchSubset.append(row)


      if matchSubset:
         matchedObjects.append([matches[0][0], matchSubset, matches[0][2]])

      return searchDone, matchedObjects


   def crossmatchWithFaintSDSSDR9PhotoStars(self, conn, objectRow, radius = 3.4):
      """crossmatchWithFaintSDSSDR9PhotoStars.

      Args:
          conn:
          objectRow:
          radius:
      """

      searchDone, matches = searchCatalogue(conn, [objectRow], 'tcs_cat_v_sdss_dr9_stars', radius = radius)

      matchedObjects = []
      matchSubset = []
      if searchDone and matches:
         for row in matches[0][1]:
            rMag = row[1]["petroMag_r"]

            if rMag > 21.0:
               matchSubset.append(row)


      if matchSubset:
         matchedObjects.append([matches[0][0], matchSubset, matches[0][2]])

      return searchDone, matchedObjects



   # 2012-10-12 KWS Note the implementation of the new algorithm (also using r instead of g).
   #                Left the old algorithm commented out for reference for the time being.
   def crossmatchWithSDSSPhotoStars(self, conn, objectRow, radius = 19.0):
      """
         Early in the PS1 survey we decided to plot every detection in the SDSS footprint
         classified as an 'orphan' against distance from and magnitude of SDSS objects.
         This should be an even distribution, but in fact there is a concentration of bright
         SDSS stars at the beginning of the plot (so the object should have been classified
         as a "star".

         This is an attempt to correctly classify the objects using simple straight line
         cuts marking the boundaries of the distribution.

         In the distribution, the stars beyond about 19 arcsec are randomly scattered,
         hence the choice of initial separation. (See Star-Orphan separation plot.)

         In the original algorithm we marked two straight lines.  In this one, one of
         the lines is eliminated and a simple vertical cut (at 1.0 arcsec).

         NOTE:  We should probably change the slope of lineTwo so that the vertical
                (1.0 arcsec) line crosses lineTwo in a sensible place.  (At the moment
                it doesn't cross in the right place.) 
      """

      searchDone, matches = searchCatalogue(conn, [objectRow], 'tcs_sdss_stars_cat', radius = radius)

      matchedObjects = []
      matchSubset = []
      if searchDone and matches:
         for row in matches[0][1]:
            rMag = row[1]["petroMag_r"]
            separation = row[0]

            #lineOne = (-13/4)*separation+30+26/4
            lineTwo = (-4/13)*separation+17+24/13

            if rMag < 13.0:
               matchSubset.append(row)
            elif rMag >= 13.0 and rMag < 21.0:
               if rMag < lineTwo:
                   matchSubset.append(row)
            elif rMag >= 21.0 and separation < 1.0:
               matchSubset.append(row)

            #if gMag < 13.0:
            #   matchSubset.append(row)
            #elif gMag > 17.0:
            #   if gMag < lineOne:
            #       matchSubset.append(row)
            #elif gMag > 13.0 and gMag < 17.0:
            #   if gMag < lineTwo:
            #       matchSubset.append(row)



      if matchSubset:
         matchedObjects.append([matches[0][0], matchSubset, matches[0][2]])

      return searchDone, matchedObjects


   def crossmatchWithSDSSDR9PhotoStars(self, conn, objectRow, radius = 19.0):
      """crossmatchWithSDSSDR9PhotoStars.

      Args:
          conn:
          objectRow:
          radius:
      """

      searchDone, matches = searchCatalogue(conn, [objectRow], 'tcs_cat_v_sdss_dr9_stars', radius = radius)

      matchedObjects = []
      matchSubset = []
      if searchDone and matches:
         for row in matches[0][1]:
            rMag = row[1]["petroMag_r"]
            separation = row[0]

            #lineTwo = (-4/13)*separation+17+24/13

            # Line passes through (x,y) = (2.5,18) and (19,13)
            lineTwo = -((18-13)/(19-2.5))*separation + 13 + 19*((18-13)/(19-2.5))

            if rMag < 13.0:
               matchSubset.append(row)
            elif rMag >= 13.0 and rMag < 18.0:
               if rMag < lineTwo:
                   matchSubset.append(row)
            elif rMag >= 18.0 and separation < 2.5:
               matchSubset.append(row)

      if matchSubset:
         matchedObjects.append([matches[0][0], matchSubset, matches[0][2]])

      return searchDone, matchedObjects



   def crossmatchWithFaint2MASSStars(self, conn, objectRow, radius = 1.0):
      """crossmatchWithFaint2MASSStars.

      Args:
          conn:
          objectRow:
          radius:
      """

      searchDone, matches = searchCatalogue(conn, [objectRow], 'tcs_cat_v_2mass_psc_noextended', radius = radius)

      matchedObjects = []
      matchSubset = []
      if searchDone and matches:
         for row in matches[0][1]:
            JMag = row[1]["j_m"]

            if JMag and JMag > 18.0:
               matchSubset.append(row)

      if matchSubset:
         matchedObjects.append([matches[0][0], matchSubset, matches[0][2]])

      return searchDone, matchedObjects


   def crossmatchWithFaintGSCStars(self, conn, objectRow, radius = 1.0):
      """crossmatchWithFaintGSCStars.

      Args:
          conn:
          objectRow:
          radius:
      """

      # NOTE: Not all mags are set all the time in GSC.  This only works if the VMag value is set.

      searchDone, matches = searchCatalogue(conn, [objectRow], 'tcs_cat_v_guide_star_ps', radius = radius)

      matchedObjects = []
      matchSubset = []
      if searchDone and matches:
         for row in matches[0][1]:
            VMag = row[1]["VMag"]

            if VMag and VMag > 20.0:
               matchSubset.append(row)

      if matchSubset:
         matchedObjects.append([matches[0][0], matchSubset, matches[0][2]])

      return searchDone, matchedObjects


   def crossmatchWithBright2MASSStars(self, conn, objectRow, radius = 15.0, jMagThreshold = 12.0):
      """
      Crossmatch against Bright 2MASS Catalogue Stars
      """

      searchDone, matches = searchCatalogue(conn, [objectRow], 'tcs_cat_v_2mass_psc_noextended', radius = radius)

      matchedObjects = []
      matchSubset = []
      if searchDone and matches:
         for row in matches[0][1]:
            JMag = row[1]["j_m"]

            if JMag and JMag < jMagThreshold:
               matchSubset.append(row)

      if matchSubset:
         matchedObjects.append([matches[0][0], matchSubset, matches[0][2]])

      return searchDone, matchedObjects


   def crossmatchWithBrightGSCStars(self, conn, objectRow, radius = 15.0, vMagThreshold = 13.0):
      """
      Crossmatch against Bright GuideStar Catalogue Stars
      """

      # NOTE: Not all mags are set all the time in GSC.  This only works if the VMag value is set.

      searchDone, matches = searchCatalogue(conn, [objectRow], 'tcs_cat_v_guide_star_ps', radius = radius)

      matchedObjects = []
      matchSubset = []
      if searchDone and matches:
         for row in matches[0][1]:
            VMag = row[1]["VMag"]

            if VMag and VMag < vMagThreshold:
               matchSubset.append(row)

      if matchSubset:
         matchedObjects.append([matches[0][0], matchSubset, matches[0][2]])

      return searchDone, matchedObjects


   def crossmatchWithBrightSDSSStars(self, conn, objectRow, radius = 15.0, rMagThreshold = 13.0):
      """
      Crossmatch against Bright SDSS Catalogue Stars
      """

      # NOTE: Not all mags are set all the time in GSC.  This only works if the VMag value is set.

      searchDone, matches = searchCatalogue(conn, [objectRow], 'tcs_cat_v_sdss_dr9_stars', radius = radius)

      matchedObjects = []
      matchSubset = []
      if searchDone and matches:
         for row in matches[0][1]:
            rMag = row[1]["petroMag_r"]

            if rMag and rMag < rMagThreshold:
               matchSubset.append(row)

      if matchSubset:
         matchedObjects.append([matches[0][0], matchSubset, matches[0][2]])

      return searchDone, matchedObjects


   # 2013-05-13 KWS Introduce a slope to check against
   def crossmatchWithBrightStars(self, conn, objectRow, minRadius = 15.0, maxRadius = 40.0, minMag = 12.0, maxMag = 9.0, catalogue = 'tcs_cat_sdss_dr9_stars_galaxies', magColumn = 'petroMag_r'):
      """
      Crossmatch against Bright Stars
      """

      searchDone, matches = searchCatalogue(conn, [objectRow], catalogue, radius = maxRadius)

      m = (minMag - maxMag) / (minRadius - maxRadius)  # gradient
      c = maxMag - m * maxRadius                       # intercept

      matchedObjects = []
      matchSubset = []
      if searchDone and matches:
         for row in matches[0][1]:
            mag = row[1][magColumn]
            separation = row[0]
            projectedMag = m * separation + c

            if separation <= minRadius and mag and mag < minMag:
               matchSubset.append(row)
               print("\t\tSimple Match: sep = %.3f, mag = %.3f" % (separation, mag))
            elif separation > minRadius and mag and mag < projectedMag:
               print("\t\tProjected Match: sep = %.3f, mag = %.3f, projected mag = %.3f" % (separation, mag, projectedMag))
               matchSubset.append(row)

      if matchSubset:
         matchedObjects.append([matches[0][0], matchSubset, matches[0][2]])

      return searchDone, matchedObjects


# 2012-10-09 KWS Created a concrete method, so removed the necessity for this to be
#                an abstract class.  Left it commented out for future reference.

# Abstract base class - python >= 2.6
# import abc

class SearchAlgorithm(object):
    """A generic search algorithm (abstract) class - designed to be subclassed"""

    # __metaclass__ = abc.ABCMeta

    # @abc.abstractmethod
    def searchField(self, conn, objectRow):
        """
        Abstract method.  Receives two database connections in the event that
        the catalogue database is not accessible via views to the local DB.
        We need to also pass in the local DB connection to facilitate updates.
        """
        return


    # 2013-10-17 KWS Moved these methods from classifierAlgorithmFactory to here.
    def updateTransientObjectType(self, conn, transientObjectId, objectType):
        """
        The default version of this class updates tcs_transient_objects
        in the Pan-STARRS1 database.  The method should be overridden in
        the PESSTO subclass.
        """

        print("Updating transient object")
        rowsUpdated = 0

        try:
            cursor = conn.cursor(MySQLdb.cursors.DictCursor)

            cursor.execute ("""
                update tcs_transient_objects
                set object_classification = %s
                where id = %s
                and object_classification != %s
            """, (objectType, transientObjectId, objectType))

            rowsUpdated = cursor.rowcount

            # Did we update any transient object rows? If not issue a warning.
            if rowsUpdated == 0:
                print("No transient object entries were updated.")

            cursor.close ()


        except MySQLdb.Error as e:
            print("Error %d: %s" % (e.args[0], e.args[1]))

        return rowsUpdated



    def deleteCrossmatch(self, conn, transientObjectId):
        """
        Remove any previous crossmatches associated with this object. Leaves any FGSS
        crossmatches (association_type not null) intact.
        The method is designed to be used in BOTH Pan-STARRS1 and PESSTO (where the
        crossmatch tables are deliberately created to be identical) so there should
        be no need to override this method.
        """

        print("Deleting previous crossmatches for this object")

        try:
            cursor = conn.cursor(MySQLdb.cursors.DictCursor)
            cursor.execute ("""
                delete from tcs_cross_matches
                where transient_object_id = %s
                and association_type is null
                """, (transientObjectId,))

            cursor.close ()

        except MySQLdb.Error as e:
            print(e)
#            if e[0] == 1142: # Can't delete - don't have permission
#                print("Can't delete.  User doesn't have permission.")
#            else:
#                print(e)

        cursor.close ()
        return conn.affected_rows()


    def insertCrossmatch(self, conn, crossmatch):
        """
        Insert the crossmatch data into the database.
        The method is designed to be used in BOTH Pan-STARRS1 and PESSTO (where the
        crossmatch tables are deliberately created to be identical) so there should
        be no need to override this method.
        """

        # +----------------------+----------------------+------+-----+---------+----------------+
        # | Field                | Type                 | Null | Key | Default | Extra          |
        # +----------------------+----------------------+------+-----+---------+----------------+
        # | transient_object_id  | bigint(20) unsigned  | NO   | MUL | NULL    |                | 
        # | catalogue_object_id  | varchar(30)          | NO   | MUL | NULL    |                | 
        # | catalogue_table_id   | smallint(5) unsigned | NO   |     | NULL    |                | 
        # | search_parameters_id | tinyint(3) unsigned  | NO   |     | NULL    |                | 
        # | separation           | double               | YES  | MUL | NULL    |                | 
        # | id                   | bigint(20) unsigned  | NO   | PRI | NULL    | auto_increment | 
        # | z                    | double               | YES  |     | NULL    |                | 
        # | scale                | double               | YES  |     | NULL    |                | 
        # | distance             | double               | YES  |     | NULL    |                | 
        # | distance_modulus     | double               | YES  |     | NULL    |                | 
        # +----------------------+----------------------+------+-----+---------+----------------+

        print("Inserting crossmatch")

        try:
            cursor = conn.cursor(MySQLdb.cursors.DictCursor)

            cursor.execute ("""
                insert into tcs_cross_matches (
                   transient_object_id,
                   catalogue_object_id,
                   catalogue_table_id,
                   separation,
                   z,
                   scale,
                   distance,
                   distance_modulus,
                   search_parameters_id)
                values (
                   %s,
                   %s,
                   %s,
                   %s,
                   %s,
                   %s,
                   %s,
                   %s,
                   %s)
                """, (crossmatch["transientObjectId"], crossmatch["catalogueObjectId"], crossmatch["catalogueTableId"], crossmatch["separation"], crossmatch["z"], crossmatch["scale"], crossmatch["distance"], crossmatch["distanceModulus"], crossmatch["searchParametersId"]))

            cursor.close ()

        except MySQLdb.Error as e:
            print(e)
#            if e[0] == 1142: # Can't insert - don't have permission
#                print("Can't insert.  User doesn't have permission.")
#            else:
#                print(e)

        cursor.close ()
        return conn.insert_id()

