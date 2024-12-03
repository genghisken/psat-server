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
import numpy as np

# 2024-05-23 KWS New VRA code for calculating features.
from st3ph3n.vra.dataprocessing import FeaturesSingleSource
from st3ph3n.vra.scoring import ScoreAndRank

# 2024-06-24 KWS Use the st3ph3n API code to write the results.
#from st3ph3n.utils import api as vraapi
from atlasapiclient import client as atlasapiclient

import os
    
def insertVRAEntry(API_CONFIG_FILE, objectId, pReal, pGal, rank, debug = False):
    payload = {'objectid': objectId, 'preal': pReal, 'pgal': pGal, 'rank': rank, 'debug': debug}
    writeto_vra = atlasapiclient.WriteToVRAScores(api_config_file = API_CONFIG_FILE, payload=payload)
    writeto_vra.get_response()

def insertVRATodo(API_CONFIG_FILE, objectId):
    payload = {'objectid': objectId}
    writeto_todo = atlasapiclient.WriteToToDo(api_config_file = API_CONFIG_FILE, payload=payload)
    writeto_todo.get_response()

def insertVRARank(API_CONFIG_FILE, objectId, rank):
    payload = {'objectid': objectId, 'rank': rank}
    writeto_rank = atlasapiclient.WriteToVRARank(api_config_file = API_CONFIG_FILE, payload=payload)
    writeto_rank.get_response()


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

    # 2024-03-28 KWS Pandas will assume float if the data types are not specified.
    # 1. Read the CSV for the RB scores. This gives us the atlas_object_id. Allows us to crop things that don't make it to the eyeball list.
    data = pd.read_csv(options.rbscorescsv, names=['objectid', 'score'], dtype={'objectid': str, 'score': float})
    if options.rbthreshold:
        data = data[data.score > float(options.rbthreshold)]

    #data = readGenericDataFile(options.rbscorescsv, fieldnames = ['objectid', 'score'], delimiter = ',')

    if data.shape[0] == 0:
        print("There are no objects to insert into the VRA table.")
        return 1


    # 2. For each atlas ID
    #      call the FeaturesSingleSource pipeline which interrogates the API and stores all the lightcurve features from last -N days (100)
    #      append the features to the main feature list

    atlas_id_tns_xm = []
    feature_list = []
    for _atlas_id in data.objectid.values:
        feature_maker = FeaturesSingleSource(atlas_id=_atlas_id)
        feature_maker.make_day1_features()
        feature_list.append(feature_maker.features)

        # Keep a record of transients with TNS crossmatches.
        if len(feature_maker.lcpipes.data.data['tns_crossmatches']) > 0:
            atlas_id_tns_xm.append(_atlas_id)

    # 3. Make dataframe with our feature list.
    features_df = pd.DataFrame(np.array(feature_list), columns = feature_maker.feature_names)


    # 4. instantiate score and rank object with the features dataframe
    #    This calculates pReal and pGal
    s_a_r = ScoreAndRank(features_df, model_type='day1', model_name='bmo')

    # 5. Make the payload to write pReal, pGal into the tcs_vra_scores table
    # list of real scores and gal scores
    s_a_r.real_scores.T[1]
    s_a_r.gal_scores.T[1]

    #for i in range(data.shape[0]):
    #    insertVRAEntry(apiURL, apiToken, int(data.iloc[i]['objectid']), float(data.iloc[i]['score']), debug = debug)
    #    insertVRATodo(apiURL, apiToken, int(data.iloc[i]['objectid']))
    i = 0

    # 6. Calculate ranks and for each atlas ID, rank pair write to the tcs_vra_rank table.

    s_a_r.calculate_rank()

    for atlas_id, pReal, pGal, rank in zip(data.objectid.values,
                                     s_a_r.real_scores.T[1],
                                     s_a_r.gal_scores.T[1],
                                     s_a_r.ranks):
        insertVRAEntry(configFile, atlas_id, pReal, pGal, rank, debug = debug)
        insertVRATodo(configFile, atlas_id)
        i += 1
        if debug:
            continue
        else:
            insertVRARank(configFile, atlas_id, rank)

    # Set the rank to 10 for objects with a TNS crossmatch.
    for atlas_id in atlas_id_tns_xm:
        rank = 10
        insertVRAEntry(configFile, atlas_id, None, None, rank, debug = debug)
        if debug:
            continue
        else:
            insertVRARank(configFile, atlas_id, rank)

    #for row in data:
    #    #print(row['objectid'], row['score'])
    #    insertVRAEntry(apiURL, apiToken, row['objectid'], row['score'], debug = debug)

    print("%d objects inserted into the VRA Scores and Todo tables." % i)
    return 0
    


if __name__ == '__main__':
    main()
