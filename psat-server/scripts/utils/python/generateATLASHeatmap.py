#!/usr/bin/env python
"""Generate an ATLAS Heatmap from specified whole MJD nights.

Usage:
  %s <configfile> <mjdList>... [--site=<site>] [--resolution=<resolution>] [--update] [--useddc] [--trim] [--multiplier=<multiplier>]
  %s (-h | --help)
  %s --version

Options:
  -h --help                  Show this screen.
  --version                  Show version.
  --site=<site>              ATLAS site [default: 01a]
  --resolution=<resolution>  Heatmap resultion [default: 128]
  --update                   Write the map into the database.
  --useddc                   Get the info from the DDC files, not the database.
  --trim                     Try and exclude outlier exposures that contain too many detections.
  --multiplier=<multiplier>  Multiplier of the median used to act as a mask [default: 1.5].

E.g.:
  %s ../../../../../atlas/config/config4_db1_readonly.yaml 59781 59782 59785 59786 --site=03a
"""

import sys
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt
import sys, os, shutil, re, csv
from gkutils.commonutils import dbConnect, Struct, cleanOptions, calculateHeatMap
import MySQLdb


def getDetectionsForHeatmap(conn, site, mjdList):

    try:
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)

        sqlSelect = 'select distinct obs, det_id, x, y from atlas_metadataddc m, atlas_detectionsddc d where d.atlas_metadata_id = m.id and x >= 0 and x < 10560 and y >= 0 and y < 10560 and '
        sqlOrPhrase = '(%s)'
        orClause=' or '.join(map(lambda x: 'm.obs like concat(%s, \'%%%%\')', mjdList))
        sql = sqlSelect + sqlOrPhrase % orClause

        #print(sql % tuple([str(site) + str(mjd) for mjd in mjdList]))

        cursor.execute (sql, tuple([str(site) + str(mjd) for mjd in mjdList]))

        resultSet = cursor.fetchall ()
        cursor.close ()

    except MySQLdb.Error as e:
        sys.stderr.write("Error %d: %s\n" % (e.args[0], e.args[1]))
        sys.exit (1)

    return resultSet

def delete_atlas_map(conn, site):
    try:
        cursor = conn.cursor (MySQLdb.cursors.DictCursor)

        cursor.execute ("""
            delete from atlas_heatmaps
            where site = %s
            """, (site,))

    except MySQLdb.Error as e:
        sys.stderr.write("Error %d: %s\n" % (e.args[0], e.args[1]))

    cursor.close ()
    conn.commit()
    return

def insert_map_row(conn, site, region, ndet):
    """
    Add the Map row to the database

    :param conn: database connection
    :param site:
    :param region:
    :param ndet:
    :return: insert id

    """
    import MySQLdb

    try:
        cursor = conn.cursor (MySQLdb.cursors.DictCursor)

        cursor.execute ("""
             insert into atlas_heatmaps (site, region, ndet)
             values (%s, %s, %s)
             """, (site, region, ndet))

    except MySQLdb.Error as e:
        sys.stderr.write("Error %d: %s\n" % (e.args[0], e.args[1]))

    cursor.close ()
    return conn.insert_id()

def insert_atlas_map(conn, matrix, site):
    delete_atlas_map(conn, site)
    for region, ndet in enumerate(matrix.flatten()):
        insert_map_row(conn, site, region, ndet)
    return

# ###########################################################################################
#                                         Main program
# ###########################################################################################

def main():
    """main.
    """
    opts = docopt(__doc__, version='0.1')
    opts = cleanOptions(opts)
    options = Struct(**opts)

    configFile = options.configfile

    import numpy as n
    import yaml
    with open(configFile) as yaml_file:
        config = yaml.load(yaml_file)

    username = config['databases']['local']['username']
    password = config['databases']['local']['password']
    database = config['databases']['local']['database']
    hostname = config['databases']['local']['hostname']


    conn = dbConnect(hostname, username, password, database)
    # 2023-03-13 KWS MySQLdb disables autocommit by default. Switch it on globally.
    conn.autocommit(True)


    detections = getDetectionsForHeatmap(conn, options.site, options.mjdList)
    print(len(detections))

    mat = calculateHeatMap(detections, resolution = int(options.resolution))

    median = n.median(mat['matrix'])
    mask = float(options.multiplier) * median
    count = n.count_nonzero(mat['matrix'] > mask)
    proportionPercentage = count/(mat['matrix'].shape[0]*mat['matrix'].shape[1]) * 100.0

    print ("Mask percentage = %.2f%%" % (proportionPercentage))

    if options.update:
        # Delete the previous map for that ATLAS site.
        delete_atlas_map(conn, options.site)
        # Add the new map for the specified site.
        insert_atlas_map(conn, mat['matrix'], options.site)


    conn.close()

if __name__ == '__main__':
    main()
