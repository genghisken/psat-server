#!/usr/bin/env python
"""Generate the stamps for ATLAS objects given name, ra and dec. Assumes the exposure hits are pre-generated.

Usage:
  %s <objectsfile> <exposuresfile> [--stampsize=<stampsize>] [--stamplocation=<location>] [--test] [--edge]
  %s (-h | --help)
  %s --version

Options:
  -h --help                    Show this screen.
  --version                    Show version.
  --test                       Just do a quick test.
  --edge                       Try and cutout stamps close to the edge.
  --stampsize=<stampsize>      Size of the postage stamps in arcsec [default: 2400].
  --stamplocation=<location>   Default place to store the stamps. [default: /tmp]

Example:
   %s ~/atlas/titan/for_smp_inside_hosts_20260616.csv ~/atlas/titan/for_smp_inside_hosts_20260616_exposures.txt --stamplocation=/home/ksmith/atlas/titan --edge --test
"""
import sys
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt
import os, shutil, re
from gkutils.commonutils import Struct, cleanOptions, readGenericDataFile, coords_sex_to_dec

from makeATLASStamp import makeATLASStamp

def generateStamps(options):

    objects = readGenericDataFile(options.objectsfile, delimiter=',')
    exposures = readGenericDataFile(options.exposuresfile, delimiter=' ')

    objectsAndExposures = {}
    
    for obj in objects:
        exps = []
        for exp in exposures:
            if exp['name'] == obj['name']:
                exps.append(exp['exp'])
        objectsAndExposures[obj['name']] = {'ra': obj['ra'], 'dec': obj['dec'], 'exps': exps}


    for obj, values in objectsAndExposures.items():
        for exp in values['exps']:

            path = options.stamplocation + '/' + obj
            filename = path + '/' + obj + '_' + os.path.basename(exp).split('.')[0] + '.fits'
            if options.test:
                print(obj, values['ra'], values['dec'], exp, filename)

            stampopts = {}

            stampopts['outputfile'] = filename
            stampopts['test'] = options.test
            stampopts['edge'] = options.edge
            stampopts['stampsize'] = options.stampsize
            stampopts['stamplocation'] = options.stamplocation

            stampoptions = Struct(**stampopts)

            if not options.test:
                os.makedirs(path, exist_ok=True)
                makeATLASStamp(stampoptions)


def main(argv = None):
    opts = docopt(__doc__, version='0.1')
    opts = cleanOptions(opts)
    options = Struct(**opts)

    generateStamps(options)

if __name__=='__main__':
    main()

