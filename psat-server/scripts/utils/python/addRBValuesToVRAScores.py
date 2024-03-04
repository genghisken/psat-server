#!/usr/bin/env python
"""Insert newly calculated RB scores into the tcs_vra_scores table via the API.

Usage:
  %s <apiConfigFile> <rbscorescsv>
  %s (-h | --help)
  %s --version

Options:
  -h --help                         Show this screen.
  --version                         Show version.

E.g.:
  %s ../../../../../atlas/config/api_config_file.yaml /tmp/ml_scores.csv
"""

import sys
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt
import os, shutil, re, csv, subprocess
from gkutils.commonutils import Struct, cleanOptions, dbConnect, coords_dec_to_sex, getDateFractionMJD, readGenericDataFile
import MySQLdb

import requests
import json
import random
import yaml

    
def insertVRAEntry(apiURL, apiToken, objectId, rbScore):
    headers = { 'Authorization': 'Token %s' % apiToken }
    
    pReal = rbScore
    
    data = {'objectid': objectId, 'preal': pReal, 'debug': True}
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


    configFile = options.apiConfigFile

    with open(configFile) as yaml_file:
        config = yaml.safe_load(yaml_file)

    apiURL = config['api']['url']
    apiToken = config['api']['token']

    data = readGenericDataFile(options.rbscorescsv, fieldnames = ['objectid', 'score'], delimiter = ',')

    for row in data:
        #print(row['objectid'], row['score'])
        insertVRAEntry(apiURL, apiToken, row['objectid'], row['score'])


if __name__ == '__main__':
    main()
