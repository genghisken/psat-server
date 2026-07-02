#!/usr/bin/env python
"""Make an ATLAS stamp given the RA and Dec and exposure ID (a wrapper for ATLAS pix2sky and monsta)

Usage:
  %s <exposure> <coords> [--outputfile=<outputfile>] [--stampSize=<n>] [--imageType=<type>] [--stamplocation=<location>] [--test]
  %s (-h | --help)
  %s --version

Options:
  -h --help                    Show this screen.
  --version                    Show version.
  --test                       Just do a quick test.
  --stampSize=<n>              Size of the postage stamps in pixels [default: 200].
  --outputfile=<outputfile>    The output filename [default: stamp]
  --stamplocation=<location>   Default place to store the stamps. [default: /tmp]

Example:
   %s /atlas/red/02a/60781/02a60781o0467o.fits.fz 226.12135656667,21.429724244444 --stampSize=400 --imageType=red --stamplocation=/tmp
"""
import sys
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt
import os, MySQLdb, shutil, re
from gkutils.commonutils import Struct, cleanOptions, readGenericDataFile, coords_sex_to_dec

import os
import numpy as np
from math import log10
from pstamp_utils import getATLASxyFromRaDec
from image_utils import getMonstaPostageStamp


def makeATLASStamp(options):

    ra,dec=options.coords.split(',')

    if options.imageType not in ["diff", "red"]:
        sys.exit("Invalid image type")

    try:
        ra = float(ra)
        dec = float(dec)
    except ValueError:
        try:
            ra, dec = coords_sex_to_dec(ra, dec)
        except Exception as e:
            sys.exit("Can't parse the coordinates.")

    stampSize = int(options.stampSize)

    ccdSizex = 10560
    ccdSizey = 10560
    if '05r' in options.exposure:
        # Use rectangular footprint of TDO
        #nx, ny, scale = (9576, 6376, 1.256)
        ccdSizex = 9576
        ccdSizey = 6376

    x,y = getATLASxyFromRaDec(exposure, ra, dec)
    if x is not None and y is not None and x >= 0 and x <= 0 and x <= ccdSizex and y <= ccdSizey:
        # Use the default monsta script so we get the JPEGs too.
        getMonstaPostageStamp(filename, options.stamplocation + '/' + options.outputfile, x, y, stampSize, monstaScript = '/atlas/lib/monsta/subarray.pro', ccdSizex = ccdSizex, ccdSizey = ccdSizey)
    else:
        print("Unable to produce stamp.")

    conn.close ()


def main(argv = None):
    opts = docopt(__doc__, version='0.1')
    opts = cleanOptions(opts)
    makeATLASStamp(options)

if __name__=='__main__':
    main()

    
