#!/usr/bin/env python
"""Crossmatch GW events using Dave's skytag code.

Usage:
  %s <configFile> <gwEventMap> <survey> [<candidate>...] [--list=<list>] [--customlist=<customlist>] [--timedeltas]
  %s (-h | --help)
  %s --version

  Survey must be panstarrs | atlas

Options:
  -h --help              Show this screen.
  --version              Show version.
  --list=<list>          List ID [default: 4].
  --customlist=<list>    Custom list ID [default: 4].
  --timedeltas           Pull out the earliest MJD (if present) for time delta calculations.


Example:
  %s /tmp/config.yaml /tmp/bayestar.fits atlas --list=2
  %s /tmp/config.yaml /tmp/lalmap.fits panstarrs --list=5
"""
import sys
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt
import os
from gkutils.commonutils import Struct, cleanOptions, dbConnect, splitList, parallelProcess

# Need to set the PYTHONPATH to find the stamp downloader. These queries should be moved to
# a common utils package. These are also ATLAS only. Need equivalents for Pan-STARRS.
from pstamp_utils import getATLASObjectsByList, getATLASObjectsByCustomList
from pstamp_utils import getObjectsByList as getPSObjectsByList
from pstamp_utils import getObjectsByCustomList as getPSObjectsByCustomList

from skytag.commonutils import prob_at_location

def crossmatchGWEvents(options):

    import yaml
    with open(options.configFile) as yaml_file:
        config = yaml.safe_load(yaml_file)

    username = config['databases']['local']['username']
    password = config['databases']['local']['password']
    database = config['databases']['local']['database']
    hostname = config['databases']['local']['hostname']


    conn = dbConnect(hostname, username, password, database)
    conn.autocommit(True)

    objectList = []

    if options.candidate is not None and len(options.candidate) > 0:
        for cand in options.candidate:
            if options.survey == 'panstarrs':
                obj = getPSObjectsByList(conn, objectId = int(cand))
                if obj:
                    objectList.append(obj)
            else:
                obj = getATLASObjectsByList(conn, objectId = int(cand))
                if obj:
                    objectList.append(obj)

    else:

        if options.customlist is not None:
            if int(options.customlist) > 0 and int(options.customlist) < 100:
                customList = int(options.customlist)
                if options.survey == 'panstarrs':
                    objectList = getPSObjectsByCustomList(conn, customList, processingFlags = 0)
                else:
                    objectList = getATLASObjectsByCustomList(conn, customList, processingFlags = 0)
            else:
                print("The list must be between 1 and 100 inclusive.  Exiting.")
                sys.exit(1)
        else:
            if options.detectionlist is not None:
                if int(options.detectionlist) >= 0 and int(options.detectionlist) < 9:
                    detectionList = int(options.detectionlist)
                    if options.survey == 'panstarrs':
                        objectList = getPSObjectsByList(conn, listId = detectionList, processingFlags = 0)
                    else:
                        objectList = getATLASObjectsByList(conn, listId = detectionList, processingFlags = 0)
                else:
                    print("The list must be between 0 and 9 inclusive.  Exiting.")
                    sys.exit(1)

    print("LENGTH OF OBJECTLIST = ", len(objectList))

    ids = []
    ras = []
    decs = []
    mjds = []

    for candidate in objectList:
        if options.timedeltas:
            if candidate['earliest_mjd']:
                ids.append(candidate['id'])
                ras.append(candidate['ra'])
                decs.append(candidate['dec'])
                mjds.append(candidate['earliest_mjd'])
        else:
            ids.append(candidate['id'])
            ras.append(candidate['ra'])
            decs.append(candidate['dec'])


    print(ras)
    print()
    print(decs)
    print()
    print(mjds)
    print()
    print(ids)
    print()

    probs = prob_at_location(
        ra=ras,
        dec=decs,
        mjd=mjds,
        mapPath=options.gwEventMap)

    print(probs)

    conn.close()


def main(argv = None):
    """main.

    Args:
        argv:
    """
    opts = docopt(__doc__, version='0.1')
    opts = cleanOptions(opts)

    # Use utils.Struct to convert the dict into an object for compatibility with old optparse code.
    options = Struct(**opts)
    crossmatchGWEvents(options)


if __name__ == '__main__':
    main()
