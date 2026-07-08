#!/usr/bin/env python
"""ATLAS TNS daemon code. This code loads up a daemon that listens on a configured
port for TNS Submit and Results messages.

Usage:
  %s <configfile> <action> [--server=<server>] [--port=<port>] [--pidfile=<pidfile>] [--sandbox] [--internalids] [--daemonErrFile=<daemonErrFile>] [--daemonOutFile=<daemonOutFile>] [--terminal] [--logfile=<logfile>]
  %s (-h | --help)
  %s --version

Options:
  -h --help                         Show this screen.
  --version                         Show version.
  --port=<port>                     The port to listen on [default: 9998].
  --server=<server>                 The IP address to listen on [default: 127.0.0.1].
  --sandbox                         Use the TNS sandbox for testing.
  --pidfile=<pidfile>               The PID file [default: /tmp/atlastnsdaemon.pid].
  --logfile=<logfile>               PID file [default: /tmp/atlastns.log]
  --internalids                     Use the new internal_ids key to specify custom key/value pairs.
  --daemonErrFile=<daemonErrFile>   Daemon Error File - for recording unexpected errors [default: /tmp/atlastnsdaemonerr.log].
  --daemonOutFile=<daemonOutFile>   Daemon Out File - for recording unexpected output [default: /tmp/atlastnsdaemonout.log].
  --terminal                        Override the Daemon error and out files and write to the terminal. 

Example:
  python atlasTNSDaemon.py ../../../../config/config4_db1.yaml start --pidfile=/localdisk/scratch/tnsDaemon.pid --internalids

"""

import sys
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt

import os
import signal
import socket
import time
import logging
import yaml

from gkutils.commonutils import Struct, cleanOptions, dbConnect
from requestATLASTNSNames import tnsReport, COMPLETE, getSubmissionReports, tnsUpdateRequestStatus, getObjectsByList, updateTNSNames

import daemon
from daemon import pidfile


def shutdown(signum, frame):  # signum and frame are mandatory
    """Handle SIGTERM cleanly."""
    sys.stdout.write("\nOuch...\n")
    sys.exit(0)


def _send(conn, message):
    """Send a text response in a Python 3 safe way."""
    if isinstance(message, str):
        message = message.encode("utf-8")
    conn.sendall(message)


def listen(options):
    """Listen for TNS daemon commands."""

    pid = os.getpid()
    sys.stdout.write("\nListening... PID is %s\n" % pid)
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    fh = logging.FileHandler(options.logfile)
    fh.setLevel(logging.INFO)

    formatstr = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(formatstr)

    fh.setFormatter(formatter)

    logger.addHandler(fh)

    with open(options.configfile) as yaml_file:
        config = yaml.safe_load(yaml_file)

    username = config['databases']['local']['username']
    password = config['databases']['local']['password']
    database = config['databases']['local']['database']
    hostname = config['databases']['local']['hostname']

    if options.sandbox:
        tnsBaseURL = config['tns_api']['atlas']['sandbox']['baseurl']
        tnsApiKey = config['tns_api']['atlas']['sandbox']['api_key']
        tnsAuthors = config['tns_api']['atlas']['authors']
        botId = config['tns_api']['atlas']['sandbox']['bot_id']
        botName = config['tns_api']['atlas']['sandbox']['bot_name']
    else:
        tnsBaseURL = config['tns_api']['atlas']['live']['baseurl']
        tnsApiKey = config['tns_api']['atlas']['live']['api_key']
        tnsAuthors = config['tns_api']['atlas']['authors']
        botId = config['tns_api']['atlas']['live']['bot_id']
        botName = config['tns_api']['atlas']['live']['bot_name']


    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((options.server, int(options.port)))
        s.listen(1)

        while True:
            conn, addr = s.accept()
            with conn:
                logger.info('Received connection from %s', str(addr))
                try:
                    data = conn.recv(1024).decode('utf-8').strip()
                except socket.error:
                    logger.error('Something went wrong. (Connection reset?) Cannot continue. Skipping this object.')
                    continue

                if data == 'Submit':
                    dbConn = dbConnect(hostname, username, password, database)
                    dbConn.autocommit(True)

                    objectList = getObjectsByList(dbConn, 2)
                    reports = tnsReport(
                        dbConn,
                        tnsBaseURL,
                        tnsApiKey,
                        objectList,
                        ddc=True,
                        reporter=tnsAuthors,
                        botId=botId,
                        botName=botName,
                        addInternalIDs=options.internalids,
                    )
                    for row in reports:
                        logger.info('Daemon Submission - TNS report ID = %s', row)
                    dbConn.close()
                    _send(conn, 'Submitted')

                elif data == 'SubmitTest':
                    dbConn = dbConnect(hostname, username, password, database)
                    dbConn.autocommit(True)

                    objectList = getObjectsByList(dbConn, 2)
                    reports = tnsReport(
                        dbConn,
                        tnsBaseURL,
                        tnsApiKey,
                        objectList,
                        ddc=True,
                        donotsend=True,
                        reporter=tnsAuthors,
                        botId=botId,
                        botName=botName,
                        addInternalIDs=internalids,
                    )
                    for row in reports:
                        logger.info('Daemon Submission - TNS report ID = %s', row)
                    dbConn.close()
                    _send(conn, 'Submitted Test')

                elif data == 'Results':
                    dbConn = dbConnect(hostname, username, password, database)
                    dbConn.autocommit(True)

                    names = getSubmissionReports(dbConn, tnsBaseURL, tnsApiKey, botId=botId, botName=botName)
                    if not names:
                        logger.info('No reports found.')
                    else:
                        reports = updateTNSNames(dbConn, names)
                        if reports:
                            for reportId in set(reports):
                                tnsUpdateRequestStatus(dbConn, reportId, COMPLETE)
                    dbConn.close()
                    _send(conn, 'Got reports')

                else:
                    _send(conn, 'Error: Invalid message.')

                time.sleep(2)


def startDaemon(options):
    """Start the daemon using python-daemon."""
    if options.terminal:
        out = sys.stdout
        err = sys.stderr
    else:
        out = open(options.daemonOutFile, 'w')
        err = open(options.daemonErrFile, 'w')

    with daemon.DaemonContext(
        working_directory='/tmp',
        umask=0o002,
        pidfile=pidfile.TimeoutPIDLockFile(options.pidfile),
        stdout=out,
        stderr=err,
        signal_map={signal.SIGTERM: shutdown},
    ):
        listen(options)


def main(argv=None):
    opts = docopt(__doc__, version='0.1')
    opts = cleanOptions(opts)
    options = Struct(**opts)


    if options.action not in ['start', 'stop', 'restart']:
        sys.stderr.write('Valid options for action are start|stop|restart\n')
        sys.exit(1)

    try:
        port = int(options.port)
        if port < 1024:
            sys.stderr.write('Port must be > 1024\n')
            sys.exit(1)
    except ValueError:
        sys.stderr.write('Port must be an integer\n')
        sys.exit(1)

    if options.action == 'listen':
        if os.path.exists(pidfile_name):
            with open(pidfile_name, mode='r') as f:
                pid = f.read().strip()
                sys.stderr.write(
                    '\nDaemon is already running (PID = %s). Kill the existing daemon first. E.g. use the stop option.\n' % pid
                )
        else:
            listen(options)

    if options.action == 'start':
        if os.path.exists(options.pidfile):
            with open(options.pidfile, mode='r') as f:
                pid = f.read().strip()
                sys.stderr.write("\nDaemon is already running (PID = %s). Stop and restart if you want to restart it.\n" % pid)
        else:
            startDaemon(options)

    if options.action == 'stop':
        if os.path.exists(options.pidfile):
            with open(options.pidfile, mode='r') as f:
                pid = f.read().strip()
            print('Stopping daemon (PID = %s).' % pid)
            os.kill(int(pid), signal.SIGTERM)
        else:
            sys.stderr.write('\nDaemon is not running.\n')

    if options.action == 'restart':
        if os.path.exists(options.pidfile):
            with open(options.pidfile, mode='r') as f:
                pid = f.read().strip()
            print("Stopping daemon (PID = %s)." % pid)
            os.kill(int(pid), signal.SIGTERM)
            time.sleep(5)
            startDaemon(options)
        else:
            startDaemon(options)


if __name__ == '__main__':
    main()
