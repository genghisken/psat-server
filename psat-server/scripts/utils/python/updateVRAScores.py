#!/usr/bin/env python
"""Periodically update VRA scores as new data arrives and decision are made.

Usage:
  %s [--debug] [--ndays=<ndays>]
  %s (-h | --help)
  %s --version

Options:
  -h --help                         Show this screen.
  --version                         Show version.
  --debug                           Debug mode.
  --ndays=<ndays>                   Search VRA Scores this number of days before current date [default: 3].

E.g.:
  %s ../../../../../atlas/config/api_config_file.yaml /tmp/ml_scores.csv

Read the VRA table on the following conditions:
  - timestamp is less than N days ago
  - debug = False
  - output the list of ATLAS 19 digit IDs and timestamps (in pairs).

For each ID, timestamp pair:
    Fetch the lightcurve
    Check if lightcurve data more recent than timestamp (detections OR nodetections)
    if no recent data:
       Do nowt.
    if recent detection:
       preal = 1
    elif recent non-detection and non-detection (mag5sig) fainter than previous detection
       preal = 0
    elif recent non-detection and non-detection brighter than previous detection
       new row preal unchanged, new timestamp



"""

import sys
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt
import os, shutil, re, csv, subprocess
from gkutils.commonutils import Struct, cleanOptions, dbConnect, coords_dec_to_sex, getDateFractionMJD, readGenericDataFile

# 2024-03-05 KWS Need to add Heloise's code into the pythonpath.
from st3ph3n.api_utils import atlas as atlasapi

import requests
import json
import random
import yaml
import pandas as pd
from datetime import datetime, timedelta
import pkg_resources


data_path = pkg_resources.resource_filename('st3ph3n', 'data')
api_config = data_path + '/api_config_MINE.yaml'

def runUpdates(options):
    """
    Read the VRA table on the following conditions:
      - timestamp is less than N days ago
      - debug = False
      - output the list of ATLAS 19 digit IDs and timestamps (in pairs).
    """

    date_threshold = (datetime.now() - timedelta(days=float(options.ndays))).strftime("%Y-%m-%d")
    payload = {'datethreshold': date_threshold}
    request_vra_scores = atlasapi.RequestVRAScores(api_config_file=api_config, payload = payload)
    request_vra_scores.get_response()
    vra_df = pd.DataFrame(request_vra_scores.response)
    print(vra_df.head())


def main():

    opts = docopt(__doc__, version='0.1')
    opts = cleanOptions(opts)
    options = Struct(**opts)

    debug = options.debug
    if debug is None:
        debug = False

    runUpdates(options)




if __name__ == '__main__':
    main()
