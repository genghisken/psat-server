#!/usr/bin/env python
"""Quick cone search.

Usage:
  %s <configfile> <coords> [--table=<table>] [--radius=<radius>]
  %s (-h | --help)
  %s --version

Options:
  -h --help                  Show this screen.
  --version                  Show version.
  --table=<table>            Table to search [default: tcs_cat_ps_tessellation]
  --radius=<radius>          Search radius (arcsec) [default: 1]

E.g.:
  %s ../../../../../atlas/config/config_cat.yaml 0.1,2.4
"""

# If we want the skycell radius of the circle that inscribes the skycell square, the value is 1,005.5052487208 arcsec.
# sky area = 4 * pi * (180/pi)^2 * 3600 (sq arcmins) * 3600 (sq arcsec)
# Divide by 264400 skycells = area in square arcsec for one skycell
# Square root of this is the diameter of a skycell.
# Take the diagonal, divide by two = the radius. There's a bit of rounding going on,
# so rounding up the skycell diameter to 24' gives a cone radius of about 1018". Use 1020 to be sure.

import sys
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt
import os, shutil, re, csv, subprocess
from gkutils.commonutils import Struct, cleanOptions, dbConnect, CAT_ID_RA_DEC_COLS, coneSearchHTM, FULL
import MySQLdb
import math

CAT_ID_RA_DEC_COLS['tcs_cat_ps_tessellation'] = [['skycell', 'ra', 'dec'],49]

def crossmatch(conn, options):

    ra, dec = options.coords.split(',')

    skycells = []
    message, results = coneSearchHTM(float(ra), float(dec), float(options.radius), options.table, queryType = FULL, conn = conn)
    if results and len(results) >= 1:
        # We have more than one object.  No we need to merge anything. Can exit now.
        for row in results:
            match = {}
            separation = row[0]
            match['separation'] = separation
            match['skycell'] = row[1]['skycell']
            skycells.append(match)

    return skycells


def main():
    """main.
    """

    #skyarea = 4 * math.pi * (180/math.pi)**2
    #diameter = math.sqrt(skyarea * 3600 * 3600 / 264400)
    #radius = math.sqrt(2 * (diameter/2)**2)
    #print(skyarea, diameter, radius)
    opts = docopt(__doc__, version='0.1')
    opts = cleanOptions(opts)
    options = Struct(**opts)

    configFile = options.configfile

    import yaml
    with open(configFile) as yaml_file:
        config = yaml.safe_load(yaml_file)

    username = config['databases']['local']['username']
    password = config['databases']['local']['password']
    database = config['databases']['local']['database']
    hostname = config['databases']['local']['hostname']

    conn = dbConnect(hostname, username, password, database)


    skycells = crossmatch(conn, options)
    for row in skycells:
        print(row)

    conn.close()


if __name__ == '__main__':
    main()

