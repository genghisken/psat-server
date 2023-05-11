#!/usr/bin/env python
"""Check for artificial satellites using online version of sat_id.

Usage:
  %s <configfile> [<candidate>...]
  %s (-h | --help)
  %s --version

Options:
  -h --help                         Show this screen.
  --version                         Show version.

E.g.:
  %s ../../../../../atlas/config/config4_db1_readonly.yaml 1041358221282934600
"""

import sys
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt
import os, shutil, re, csv, subprocess
from gkutils.commonutils import Struct, cleanOptions, dbConnect, coords_dec_to_sex, getDateFractionMJD
import MySQLdb

observatories = {'01a': 'T08', '02a': 'T05', '03a': 'M22', '04a': 'W68'}

def getObjectInfo(conn, objectId):
    """
    Get all object info.
    """

    try:
        cursor = conn.cursor (MySQLdb.cursors.DictCursor)

        cursor.execute ("""
            SELECT d.ra,
                   d.`dec`,
                   m.mjd,
                   m.obs
            FROM atlas_detectionsddc d, atlas_metadataddc m
            where d.atlas_object_id=%s
            and d.atlas_metadata_id = m.id
            and d.dup >= 0
            and d.det != 5
            ORDER by mjd
        """, (objectId,))
        resultSet = cursor.fetchall ()

        cursor.close ()

    except MySQLdb.Error as e:
        print("Error %d: %s" % (e.args[0], e.args[1]))
        sys.exit (1)


    return resultSet






def main():
    """main.
    """
    opts = docopt(__doc__, version='0.1')
    opts = cleanOptions(opts)
    options = Struct(**opts)

    configFile = options.configfile

    import yaml
    with open(configFile) as yaml_file:
        config = yaml.load(yaml_file)

    username = config['databases']['local']['username']
    password = config['databases']['local']['password']
    database = config['databases']['local']['database']
    hostname = config['databases']['local']['hostname']

    conn = dbConnect(hostname, username, password, database)

    counter = 1
    for obj in options.candidate:
        dets = getObjectInfo(conn, int(obj))
        for det in dets:
            coords = coords_dec_to_sex(det['ra'], det['dec'], delimiter=' ', decimalPlacesRA=3, decimalPlacesDec=2)
            date = getDateFractionMJD(det['mjd'],  decimalPlaces=5)
            print("     %05d     %s %s%s                     %s" % (counter, date, coords[0], coords[1], observatories[det['obs'][0:3]]))
        counter = counter + 1
    conn.close()


if __name__ == '__main__':
    main()
