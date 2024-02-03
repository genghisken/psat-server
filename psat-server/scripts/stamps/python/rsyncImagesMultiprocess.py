#!/usr/bin/env python
"""Rsync a list of images from Hawaii - with exposureSet either specified on the command
   line or grabbed from recently downloaded ddc files. Could specifiy ddc files OR images
   OR both!

Usage:
  %s [<exposureSet>...] [--days=<n>] [--ddc] [--camera=<camera>] [--mjd=<mjd>] [--downloadthreads=<threads>] [--loglocationdownloads=<loglocationdownloads>] [--logprefixdownloads=<logprefixdownloads>]
  %s (-h | --help)
  %s --version

Options:
  -h --help                                       Show this screen.
  --version                                       Show version.
  --days=<n>                                      Number of days from today [default: 5].
  --mjd=<mjd>                                     Just the specified MJD.
  --camera=<camera>                               Camera [default: 02a].
  --downloadthreads=<threads>                     The number of threads (processes) to use [default: 5].
  --loglocationdownloads=<loglocationdownloads>   Downloader log file location [default: /tmp/]
  --logprefixdownloads=<logprefixdownloads>       Downloader log prefix [default: background_exposure_downloads]

"""
import sys
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt
import os, MySQLdb, shutil, re
from gkutils.commonutils import find, Struct, cleanOptions, getCurrentMJD, splitList, parallelProcess
import gc
from makeATLASStamps import doRsync
import queue
from random import shuffle
import datetime


def workerImageDownloader(num, db, listFragment, dateAndTime, firstPass, miscParameters):
    """thread worker function"""
    # Redefine the output to be a log file.
    options = miscParameters[0]
    imageType = miscParameters[1]
    sys.stdout = open('%s%s_%s_%d.log' % (options.loglocationdownloads, options.logprefixdownloads, dateAndTime, num), "w")

    # Call the postage stamp downloader
    objectsForUpdate = doRsync(listFragment, imageType, ignoreExistingFiles = True)
    #q.put(objectsForUpdate)
    print("Process complete.")
    return 0


def main(argv = None):
    opts = docopt(__doc__, version='0.1')
    opts = cleanOptions(opts)

    # Use utils.Struct to convert the dict into an object for compatibility with old optparse code.
    options = Struct(**opts)

    db = []

    currentMJD = int(getCurrentMJD())

    lockfile = "/tmp/rsync_images_lockfiles_%s" % options.camera
    if os.path.exists(lockfile):
        print("Rsync in progress. Remove lockfile to restart.")
        return

    exposureSet = []
    if options.exposureSet:
        # Ignore n days value
        exposureSet = options.exposureSet
    else:
        ddcFiles = []
        if options.mjd:
            ddcFiles += find('*.ddc', '/atlas/diff/%s/%s/' % (options.camera, options.mjd))
        else:
            # 2023-01-03 KWS Add tomorrow's MJD to the list of MJDs. This is because
            #                STH starts tomorrow's MJD before midnight.
            for m in range(currentMJD - int(options.days) + 1, currentMJD + 1 + 1):
                ddcFiles += find('*.ddc', '/atlas/diff/%s/%d/' % (options.camera, m))
        if ddcFiles:
            # Produce the exposureSet list.
            exposureSet = [f.split('/')[5].split('.')[0] for f in ddcFiles]

    shuffle(exposureSet)
    # Now we have the exposureSet, we can go and get them.

    currentDate = datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")
    (year, month, day, hour, min, sec) = currentDate.split(':')
    dateAndTime = "%s%s%s_%s%s%s" % (year, month, day, hour, min, sec)

    with open(lockfile, 'w') as f:
        f.write("Rsync started on %s\n" % currentDate)

    # Download threads with multiprocessing - try 10 threads by default
    print("TOTAL OBJECTS = %d" % len(exposureSet))

    print("Downloading exposures...")

    if len(exposureSet) > 0:
        nProcessors, listChunks = splitList(exposureSet, bins = int(options.downloadthreads))

        print("%s Parallel Processing..." % (datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")))
        parallelProcess(db, dateAndTime, nProcessors, listChunks, workerImageDownloader, miscParameters = [options, 'red'], drainQueues = False)
        print("%s Done Parallel Processing" % (datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")))
        print("%s Parallel Processing..." % (datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")))
        parallelProcess(db, dateAndTime, nProcessors, listChunks, workerImageDownloader, miscParameters = [options, 'diff'], drainQueues = False)
        print("%s Done Parallel Processing" % (datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")))

        # Belt and braces. Do again, with one less thread.
        nProcessors, listChunks = splitList(exposureSet, bins = int(options.downloadthreads) - 1)

        print("%s Parallel Processing..." % (datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")))
        parallelProcess(db, dateAndTime, nProcessors, listChunks, workerImageDownloader, miscParameters = [options, 'red'], drainQueues = False)
        print("%s Done Parallel Processing" % (datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")))
        print("%s Parallel Processing..." % (datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")))
        parallelProcess(db, dateAndTime, nProcessors, listChunks, workerImageDownloader, miscParameters = [options, 'diff'], drainQueues = False)
        print("%s Done Parallel Processing" % (datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")))

    # Remove the lockfile
    os.remove(lockfile)

    return



if __name__ == '__main__':
    main()
