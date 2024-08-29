#!/usr/bin/env python
"""Refactored External Crossmatch code for Pan-STARRS.

Usage:
  %s <configfile> [<candidate>...] [--list=<listid>] [--searchRadius=<radius>] [--update] [--date=<date>] [--updateSNType] [--tnsOnly]
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
  --date=<date>                 Date threshold - no hyphens [default: 20160601]
  --tnsOnly                     Only search the TNS database.

  Example:
    %s ../../../../../ps13pi/config/config.yaml 1132030161113247300 --update --updateSNType
"""
import sys
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt
import os, MySQLdb, shutil, re
from gkutils.commonutils import find, Struct, cleanOptions, dbConnect, calculateRMSScatter, getAngularSeparation, coneSearchHTM, QUICK, FULL, CAT_ID_RA_DEC_COLS, PROCESSING_FLAGS
from datetime import datetime
from externalTransientCrossmatchUtils import getAtlasObjects, getPS1Objects, getCatalogueTables, deleteExternalCrossmatches, insertExternalCrossmatches, updateObjectSpecType, updateObjectSpecTypePS1, crossmatchExternalLists



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
    conn.autocommit(True)
    connCatalogues = dbConnect(cathost, catuser, catpass, catname, quitOnError = True)

    connPESSTO = dbConnect(PESSTOCATHOST, PESSTOCATUSER, PESSTOCATPASS, PESSTOCATNAME, lport = PESSTOCATPORT, quitOnError = False)


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
            dateThreshold = '%s-%s-%s' % (options.date[0:4], options.date[4:6], options.date[6:8])
        except:
            dateThreshold = '2016-06-01'

    if len(options.candidate) > 0:
        for row in options.candidate:
            object = getPS1Objects(conn, objectId = int(row))
            if object:
                candidateList.append(object)

    else:
        # Get only the ATLAS objects that don't have the 'moons' flag set.
        candidateList = getPS1Objects(conn, listId = detectionList, dateThreshold = dateThreshold)


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
                    print('UPDATE', matchrow['matched_list'], matchrow['ps1_designation'],  matchrow['external_designation'])
                    updateObjectSpecTypePS1(conn, result['id'], matchrow['type'])



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

