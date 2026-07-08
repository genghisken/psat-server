#!/usr/bin/env python
"""Pan-STARRS TNS daemon code. This code loads up a daemon that listens on a configured port for TNS Submit and Results messages.

Usage:
  %s <configfile> <action> [--server=<server>] [--port=<port>] [--pidfile=<pidfile>] [--sandbox] [--internalids] [--daemonErrFile=<daemonErrFile>] [--daemonOutFile=<daemonOutFile>] [--terminal] [--logfile=<logfile>]
  %s (-h | --help)
  %s --version

Options:
  -h --help                         Show this screen.
  --version                         Show version.
  --port=<port>                     The port to listen on [default: 9998].
  --server=<server>                 The IP address to listen on [default: 127.0.0.1].
  --pidfile=<pidfile>               The PID file [default: /tmp/panstarrstnsdaemon.pid].
  --logfile=<logfile>               PID file [default: /tmp/panstarrstns.log]
  --sandbox                         Use the TNS sandbox for testing.
  --internalids                     Use the new internal_ids key to specify custom key/value pairs.
  --daemonErrFile=<daemonErrFile>   Daemon Error File - for recording unexpected errors [default: /tmp/panstarrstnsdaemonerr.log].
  --daemonOutFile=<daemonOutFile>   Daemon Out File - for recording unexpected output [default: /tmp/panstarrstnsdaemonout.log].
  --terminal                        Override the Daemon error and out files and write to the terminal. 

Example:
  python %s ../../../../config/config.yaml start --pidfile=/nvme/1/var/tnsDaemonPS.pid

"""

import sys
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0])

from docopt import docopt
import os
import logging
import socket
import signal
import time
import yaml

import daemon
from daemon import pidfile

from gkutils.commonutils import Struct, cleanOptions, dbConnect
from requestPanSTARRSTNSNames import tnsReport, COMPLETE, getSubmissionReports, tnsUpdateRequestStatus, updateTNSNames, getSpecifiedObjects


# To kick off the script, run the following from the python directory:
#   PYTHONPATH=`pwd` python panstarrsTNSDaemon.py start


def shutdown(signum, frame):  # signum and frame are mandatory
    """Graceful shutdown handler."""
    sys.stdout.write("\nOuch...\n")
    sys.exit(0)


def _get_config_value(config, *keys, default=None):
    """Walk nested dictionaries safely and return a default if a key is missing."""
    current = config
    for key in keys:
        try:
            current = current[key]
        except KeyError:
            return default
    return current


def _recv_text(conn, size=1024):
    """Receive a socket payload and return a stripped unicode string."""
    data = conn.recv(size)
    if isinstance(data, bytes):
        return data.decode("utf-8", errors="replace").strip()
    return str(data).strip()


def _send_text(conn, message):
    """Send a response as bytes under Python 3."""
    if isinstance(message, str):
        message = message.encode("utf-8")
    conn.sendall(message)


def listen(options):
    """Listen for incoming TNS requests."""

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
        tnsBaseURL = config['tns_api']['ps1']['sandbox']['baseurl']
        tnsApiKey = config['tns_api']['ps1']['sandbox']['api_key']
        botId = config['tns_api']['ps1']['sandbox']['bot_id']
        botName = config['tns_api']['ps1']['sandbox']['bot_name']
    else:
        tnsBaseURL = config['tns_api']['ps1']['live']['baseurl']
        tnsApiKey = config['tns_api']['ps1']['live']['api_key']
        botId = config['tns_api']['ps1']['live']['bot_id']
        botName = config['tns_api']['ps1']['live']['bot_name']

    tnsAuthors = config['tns_api']['ps1']['authors']

    supplementaryAuthors = _get_config_value(config, 'tns_api', 'ps1', 'supplementaryauthors', default='')
    supplementaryAuthorsTrigger = _get_config_value(config, 'tns_api', 'ps1', 'supplementaryauthorstrigger', default='')
    zooniverseBoilerplate = _get_config_value(config, 'tns_api', 'ps1', 'zooniverse_boilerplate', default='')

    zooniverseScoreThreshold = _get_config_value(config, 'tns_api', 'ps1', 'zooniverse_score_threshold', default=0.95)
    try:
        zooniverseScoreThreshold = float(zooniverseScoreThreshold)
    except (TypeError, ValueError):
        zooniverseScoreThreshold = 0.95

    reportingGroupId = _get_config_value(config, 'tns_api', 'ps1', 'reporting_group_id', default=4)
    try:
        reportingGroupId = int(reportingGroupId)
    except (TypeError, ValueError):
        reportingGroupId = 4

    discoveryDataSourceId = _get_config_value(config, 'tns_api', 'ps1', 'discovery_data_source_id', default=4)
    try:
        discoveryDataSourceId = int(discoveryDataSourceId)
    except (TypeError, ValueError):
        discoveryDataSourceId = 4

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((server, port))
        s.listen(1)

        while True:
            conn, addr = s.accept()
            with conn:
                logger.info('Received connection from %s', addr)

                try:
                    data = _recv_text(conn)
                except socket.error:
                    logger.error('Something went wrong reading from the socket. Skipping this object.')
                    continue

                if data.startswith('Submit '):
                    ids = data.replace('Submit ', '', 1).split()
                    try:
                        objectIds = list(map(int, ids))
                    except ValueError:
                        logger.error('IDs should be integers')
                        _send_text(conn, 'Error: Bad IDs.')
                        continue

                    if not objectIds:
                        logger.error('No objects to request TNS names for.')
                        _send_text(conn, 'Error: No IDs.')
                        continue

                    dbConn = dbConnect(hostname, username, password, database)
                    dbConn.autocommit(True)
                    try:
                        objectList = getSpecifiedObjects(dbConn, objectIds)

                        if objectList:
                            logger.info('Requesting TNS names for: %s', objectIds)
                            reports = tnsReport(
                                dbConn,
                                tnsBaseURL,
                                tnsApiKey,
                                objectList,
                                skipNonDetections=True,
                                reporter=tnsAuthors,
                                supplementaryAuthors=supplementaryAuthors,
                                supplementaryAuthorsTrigger=supplementaryAuthorsTrigger,
                                reportingGroupId=reportingGroupId,
                                discoveryDataSourceId=discoveryDataSourceId,
                                zooniverseBoilerplate=zooniverseBoilerplate,
                                zooniverseScoreThreshold=zooniverseScoreThreshold,
                                botId=botId,
                                botName=botName,
                                addInternalIDs=internalids,
                            )
                            for row in reports:
                                logger.info('Daemon Submission - TNS report ID = %s', row)
                            _send_text(conn, 'Submitted')
                        else:
                            logger.info('Error: No valid objects to submit.')
                            _send_text(conn, 'Error: Submitted Nothing')
                    finally:
                        dbConn.close()

                elif data.startswith('SubmitTest '):
                    ids = data.replace('SubmitTest ', '', 1).split()
                    try:
                        objectIds = list(map(int, ids))
                    except ValueError:
                        logger.error('IDs should be integers')
                        _send_text(conn, 'Error: Bad IDs.')
                        continue

                    if not objectIds:
                        logger.error('No objects to request TNS names for.')
                        _send_text(conn, 'Error: No IDs.')
                        continue

                    dbConn = dbConnect(hostname, username, password, database)
                    dbConn.autocommit(True)
                    try:
                        objectList = getSpecifiedObjects(dbConn, objectIds)

                        if objectList:
                            logger.info('TEST Requesting TNS names for: %s', objectIds)
                            reports = tnsReport(
                                dbConn,
                                tnsBaseURL,
                                tnsApiKey,
                                objectList,
                                skipNonDetections=True,
                                reporter=tnsAuthors,
                                supplementaryAuthors=supplementaryAuthors,
                                supplementaryAuthorsTrigger=supplementaryAuthorsTrigger,
                                donotsend=True,
                                zooniverseBoilerplate=zooniverseBoilerplate,
                                zooniverseScoreThreshold=zooniverseScoreThreshold,
                                botId=botId,
                                botName=botName,
                                addInternalIDs=internalids,
                            )
                            for row in reports:
                                logger.info('Daemon TEST Submission - TNS report ID = %s', row)
                            _send_text(conn, 'Submitted Test')
                        else:
                            logger.info('Error: No valid objects to submit.')
                            _send_text(conn, 'Error: Submitted Nothing')
                    finally:
                        dbConn.close()

                elif data == 'Results':
                    dbConn = dbConnect(hostname, username, password, database)
                    dbConn.autocommit(True)
                    try:
                        names = getSubmissionReports(dbConn, tnsBaseURL, tnsApiKey, botId=botId, botName=botName)
                        if not names:
                            logger.info('No reports found.')
                        else:
                            reports = updateTNSNames(dbConn, names)
                            if reports:
                                for reportId in set(reports):
                                    tnsUpdateRequestStatus(dbConn, reportId, COMPLETE)
                        _send_text(conn, 'Got reports')
                    finally:
                        dbConn.close()
                else:
                    logger.error('Invalid message.')
                    _send_text(conn, 'Error: Invalid message.')

                time.sleep(1)


def startDaemon(options):
    """Start the daemon process."""
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

    sandbox = options.sandbox
    pidfile_name = options.pidfile
    internalids = options.internalids

    server = '' if options.server is None else options.server

    if options.action not in ['start', 'stop', 'restart', 'listen']:
        sys.stderr.write('Valid options for action are start|stop|restart|listen\n')
        sys.exit(1)

    try:
        port = int(options.port)
        if port < 1024:
            sys.stderr.write('Port must be > 1024\n')
            sys.exit(1)
    except ValueError:
        sys.stderr.write('Port must be an integer\n')
        sys.exit(1)

    with open(options.configfile) as yaml_file:
        config = yaml.safe_load(yaml_file)


    # The daemon package expects argv to look like a simple action command.
    sys.argv = [sys.argv[0], options.action]

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
