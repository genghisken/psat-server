#!/usr/bin/env python
"""Split a data cube into separate FITS files.

Usage:
  %s <filename> [--slice=<slice>]
  %s (-h | --help)
  %s --version

Options:
  -h --help           Show this screen.
  --version           Show version.
  --slice=<slice>     Data cube slice.


Example:
  python %s /tmp/fitscube.fits --slice=0
"""
import sys
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt
import os
from gkutils.commonutils import Struct, cleanOptions
from astropy.io import fits as pf

def getCubeData(options):

    h = pf.open(options.filename)

    if options.slice:
        slice = int(options.slice)
    else:
        slice = 0

    header = h[0].header
    data = h[0].data[slice]

    outfile = os.path.basename(options.filename) + '_' +  str(slice) + '.fits'

    hdu = pf.PrimaryHDU(data)

    hdulist = pf.HDUList([hdu])
    hdulist[0].header = header

    #new_hdul.append(pf.PrimaryHDU(header=headermain))
    hdulist.writeto(outfile, overwrite=True)



def main(argv = None):
    """main.

    Args:
        argv:
    """
    opts = docopt(__doc__, version='0.1')
    opts = cleanOptions(opts)

    # Use utils.Struct to convert the dict into an object for compatibility with old optparse code.
    options = Struct(**opts)
    getCubeData(options)







if __name__ == '__main__':
    main()
