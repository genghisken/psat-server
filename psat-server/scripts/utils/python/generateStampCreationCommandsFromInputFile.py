#!/usr/bin/env python
"""Create Monsta commands to produce cutouts from an input CSV that contains nnc_id, x and y.

Usage:
  %s <filename> [--stampsize=<stampsize>] [--detectorx=<detectorx>] [--detectory=<detectory>] [--dataroot=<dataroot>]
  %s (-h | --help)
  %s --version

Options:
  -h --help                  Show this screen.
  --version                  Show version.
  --stampsize=<stampsize>    Stamp stampsize [default: 200]
  --detectorx=<detectorx>    Detector stampsize in x direction [default: 10560]
  --detectory=<detectory>    Detector stampsize in y direction [default: 10560]
  --dataroot=<dataroot>      Root location of the stamps [default: /home/ksmith/cutouts/atlas/]

Example:
  python %s /tmp/fitscube.fits --stampsize=0
"""
import sys
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt
import os
from gkutils.commonutils import Struct, cleanOptions, readGenericDataFile
from astropy.io import fits as pf

def getNNCInfo(options):

    data = readGenericDataFile(options.filename, delimiter=',')
    for row in data:
        nnc_id = row['nnc_id']
        exposure = nnc_id.split('.')[0]
        camera = exposure[0:3]
        mjd = exposure[3:8]
        x = float(row['x'])
        y = float(row['y'])
        stampsize = options.stampsize

        x1 = x - float(options.stampsize)/2
        x2 = x + float(options.stampsize)/2
        y1 = y - float(options.stampsize)/2
        y2 = y + float(options.stampsize)/2

        #os.mkdirs(options.dataroot + '/' + mjd)
        
        print ('mkdir -p ' + options.dataroot + mjd + ' && ', end = "")
        inputImageName = options.dataroot + mjd + '/' + row['atlas_object_id'] + '_' + row['detection_id'] + '_' + row['nnc_id'] + '_input.fits'
        diffImageName = options.dataroot + mjd + '/' + row['atlas_object_id'] + '_' + row['detection_id'] + '_' + row['nnc_id'] + '_diff.fits'
        refImageName = options.dataroot + mjd + '/' + row['atlas_object_id'] + '_' + row['detection_id'] + '_' + row['nnc_id'] + '_ref.fits'

        inputcommand = '/atlas/vendor/monsta/bin/monsta /home/ksmith/monsta/subarray_ken3.pro ' + '/atlas/red/' + camera + '/' + mjd + '/' + exposure + '.fits.fz ' +  inputImageName + ' ' + str(x1) + ' ' + str(x2) + ' ' + str (y1) + ' ' + str (y2) + ' ' + options.detectorx + ' ' + options.detectory
        diffcommand = '/atlas/vendor/monsta/bin/monsta /home/ksmith/monsta/subarray_ken3.pro ' + '/atlas/diff/' + camera + '/' + mjd + '/' + exposure + '.diff.fz ' + diffImageName + ' ' + str(x1) + ' ' + str(x2) + ' ' + str (y1) + ' ' + str (y2) + ' ' + options.detectorx + ' ' + options.detectory

        refcommand = '/atlas/bin/wpwarp2 -novar -nomask -nozerosat -wp ' + refImageName + ' ' + inputImageName
    
        print(diffcommand, end="")
        print(' && ', end="")
        print(inputcommand, end="")
        print(' && ', end="")
        print(refcommand)





def main(argv = None):
    """main.

    Args:
        argv:
    """
    opts = docopt(__doc__, version='0.1')
    opts = cleanOptions(opts)

    # Use utils.Struct to convert the dict into an object for compatibility with old optparse code.
    options = Struct(**opts)
    getNNCInfo(options)







if __name__ == '__main__':
    main()
