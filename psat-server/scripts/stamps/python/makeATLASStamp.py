#!/usr/bin/env python
"""Make an ATLAS stamp given the RA and Dec and exposure ID (a wrapper for ATLAS pix2sky and monsta)

Usage:
  %s <exposure> <coords> [--outputfile=<outputfile>] [--stampsize=<stampsize>] [--stamplocation=<location>] [--test]
  %s (-h | --help)
  %s --version

Options:
  -h --help                    Show this screen.
  --version                    Show version.
  --test                       Just do a quick test.
  --stampsize=<stampsize>      Size of the postage stamps in arcsec [default: 2400].
  --outputfile=<outputfile>    The output filename [default: stamp]
  --stamplocation=<location>   Default place to store the stamps. [default: /tmp]

Example:
   %s /atlas/red/02a/60781/02a60781o0467o.fits.fz 226.12135656667,21.429724244444 --stampSize=400 --stamplocation=/tmp
"""
import sys
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt
import os, shutil, re
from gkutils.commonutils import Struct, cleanOptions, readGenericDataFile, coords_sex_to_dec

import os
import numpy as np
from math import log10
from pstamp_utils import getATLASxyFromRaDec
from image_utils import getMonstaPostageStamp


def makeATLASStamp(options):

    ra,dec=options.coords.split(',')

    try:
        ra = float(ra)
        dec = float(dec)
    except ValueError:
        try:
            ra, dec = coords_sex_to_dec(ra, dec)
        except Exception as e:
            sys.exit("Can't parse the coordinates.")

    nx = 10560
    ny = 10560
    scale = 1.863


    if '05r' in options.exposure:
        # Use rectangular footprint of TDO
        nx, ny, scale = (9576, 6376, 1.256)

    stampsize = float(options.stampsize)/scale

    x,y = getATLASxyFromRaDec(options.exposure, ra, dec)
    if x is not None and y is not None and x >= 0 and y >= 0 and x <= nx and y <= ny:
        # Use the default monsta script so we get the JPEGs too.
        getMonstaPostageStamp(options.exposure, options.stamplocation + '/' + options.outputfile, x, y, stampsize, monstaScript = '/atlas/lib/monsta/subarray.pro', ccdSizex = nx, ccdSizey = ny)
    else:
        print("Unable to produce stamp.")


def main(argv = None):
    opts = docopt(__doc__, version='0.1')
    opts = cleanOptions(opts)
    options = Struct(**opts)

    makeATLASStamp(options)

if __name__=='__main__':
    main()

    
