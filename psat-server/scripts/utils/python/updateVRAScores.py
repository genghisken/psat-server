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
from gkutils.commonutils import Struct, cleanOptions, dbConnect, coords_dec_to_sex, getDateFractionMJD, readGenericDataFile, getMJDFromSqlDate

# 2024-03-05 KWS Need to add Heloise's code into the pythonpath.
from st3ph3n.api_utils import atlas as atlasapi

import requests
import json
import random
import yaml
import pandas as pd
import numpy as np
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

    date_threshold = (datetime.now() - timedelta(days=float(options.ndays))).strftime("%Y-%m-%d %H:%M:%S")
    payload = {'datethreshold': date_threshold}
    request_vra_scores = atlasapi.RequestVRAScores(api_config_file=api_config,
                                                   payload = payload,
                                                   get_response = True
                                                   )
    vra_df = pd.DataFrame(request_vra_scores.response)
    unique_transient_ids = set(vra_df.transient_object_id.values)
    transient_ids_with_decisions = set(vra_df[~vra_df.username.isnull()].transient_object_id.values)
    set_to_update = unique_transient_ids - transient_ids_with_decisions
    print(vra_df[np.isin(vra_df.transient_object_id.values, list(set_to_update))][['transient_object_id','timestamp']].drop_duplicates(subset='transient_object_id', keep='last'))

    request_data = atlasapi.RequestMultipleSourceData(api_config_file=api_config,
                                                     array_ids=np.array(list(set_to_update))[:2],
                                                     mjdthreshold = getMJDFromSqlDate(date_threshold)
                                                     )
    request_data.chunk_get_response_quiet()

    first_response = request_data.response[0]
    data_lc = pd.DataFrame(first_response['lc'])
    data_lcnondets = pd.DataFrame(first_response['lcnondets'])
    data_lcnondets['magerr'] = 0.0

    data_lc = data_lc[['mjd','filter','mag','magerr','date_inserted']]
    data_lc['det'] = 1
    data_lcnondets = data_lcnondets[['mjd','filter','mag5sig','magerr','date_inserted']]
    data_lcnondets.columns=['mjd','filter','mag','magerr','date_inserted']
    data_lcnondets['det'] = 0
    data_all = pd.concat([data_lc, data_lcnondets]).sort_values('mjd').reset_index(drop=True)
    print(data_all.head())

    #for 
    #if no recent data:
    #   Do nowt.
    #if recent detection:
    #   preal = 1
    #elif recent non-detection and non-detection (mag5sig) fainter than previous detection
    #   preal = 0
    #elif recent non-detection and non-detection brighter than previous detection
    #   new row preal unchanged, new timestamp




    first_object = request_data.response[0]
    print(len(first_object['lc']), len(first_object['lcnondets']))


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
