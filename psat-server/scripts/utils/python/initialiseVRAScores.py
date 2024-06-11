#!/usr/bin/env python
"""Insert newly calculated RB scores into the tcs_vra_scores table via the API.

Usage:
  %s <apiConfigFile> <rbscorescsv> [--debug] [--rbthreshold=<rbthreshold>]
  %s (-h | --help)
  %s --version

Options:
  -h --help                         Show this screen.
  --version                         Show version.
  --debug                           Debug mode.
  --rbthreshold=<rbthreshold>       RB Threshold (if not set will be ignored).

E.g.:
  %s ../../../../../atlas/config/api_config_file.yaml /tmp/ml_scores.csv
"""

import sys
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt
import os, shutil, re, csv, subprocess
from gkutils.commonutils import Struct, cleanOptions, dbConnect, coords_dec_to_sex, getDateFractionMJD, readGenericDataFile

import requests
import json
import random
import yaml
import pandas as pd

    
def insertVRAEntry(apiURL, apiToken, objectId, rbScore, debug = False):
    headers = { 'Authorization': 'Token %s' % apiToken }
    
    pReal = rbScore
    
    data = {'objectid': objectId, 'preal': pReal, 'debug': debug}
    url = apiURL + 'vrascores/'
    r = requests.post(url, data, headers=headers)
    objectListResponse = None
    if r.status_code == 201:
        objectListResponse = r.json()
        #print (json.dumps(objectListResponse, indent = 2))
    else:
        print('Oops, status code is', r.status_code)
        print(r.text)
    
    if objectListResponse is None:
        print("Bad response from the objectlist API")
        exit(1)


def main():
    """main.
    """
    opts = docopt(__doc__, version='0.1')
    opts = cleanOptions(opts)
    options = Struct(**opts)

    debug = options.debug
    if debug is None:
        debug = False

    configFile = options.apiConfigFile

    rbThreshold = None
    if options.rbthreshold is not None:
        rbThreshold = float(options.rbthreshold)

    assert os.path.exists(options.rbscorescsv), f"File does not exist: {options.rbscorescsv}"

    with open(configFile) as yaml_file:
        config = yaml.safe_load(yaml_file)

    apiURL = config['api']['url']
    apiToken = config['api']['token']

    # 2024-03-28 KWS Pandas will assume float if the data types are not specified.
    data = pd.read_csv(options.rbscorescsv, names=['objectid', 'score'], dtype={'objectid': str, 'score': float})
    if options.rbthreshold:
        data = data[data.score > float(options.rbthreshold)]

    #data = readGenericDataFile(options.rbscorescsv, fieldnames = ['objectid', 'score'], delimiter = ',')

    if data.shape[0] == 0:
        print("There are no objects to insert into the VRA table.")
        return 1

    i = 0
    for i in range(data.shape[0]):
        insertVRAEntry(apiURL, apiToken, int(data.iloc[i]['objectid']), float(data.iloc[i]['score']), debug = debug)


    #for row in data:
    #    #print(row['objectid'], row['score'])
    #    insertVRAEntry(apiURL, apiToken, row['objectid'], row['score'], debug = debug)

    print("%d objects inserted into the VRA Scores table." % i)
    return 0
    


if __name__ == '__main__':
    main()
