#!/usr/bin/env python
"""Periodically update VRA scores as new data arrives and decision are made.

Usage:
  %s <apiConfigFile> [--rankcolumn=<rankcolumn>] [--debug] [--ndays=<ndays>] [--quiet]
  %s (-h | --help)
  %s --version [--quiet]

Options:
  -h --help                         Show this screen.
  --version                         Show version.
  --rankcolumn=<rankcolumn>         Rank column in which to write the rank [default: rank].
  --debug                           Debug mode.
  --ndays=<ndays>                   Search VRA Scores this number of days before current date [default: 30].
  --quiet                           Use quiet mode (for writing into logs).

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

from atlasapiclient import client as atlasapiclient
from atlasvras.st3ph3n.dataprocessing import FeaturesSingleSource
from atlasvras.st3ph3n.scoreandrank import ScoreAndRank
from atlasvras.utils.exceptions import VRASaysNo
import requests
import json
import random
import yaml
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pkg_resources
from astropy.time import Time


#data_path = pkg_resources.resource_filename('st3ph3n', 'data')
#api_config = data_path + '/api_config_MINE.yaml'

EYEBALL_THRESHOLD = 7.0

def insertVRAEntry(API_CONFIG_FILE, objectId, pReal, pGal, rank, rank_column, is_gal_cand, debug = False):
    if rank_column not in ['rank', 'rank_alt1']:
        raise VRASaysNo('The rank column must be rank or rank_alt1.')
    payload = {'objectid': objectId, 'preal': pReal, 'pgal': pGal, rank_column: rank, 'is_gal_cand': is_gal_cand, 'debug': debug}
    writeto_vra = atlasapiclient.WriteToVRAScores(api_config_file = API_CONFIG_FILE, payload=payload)
    writeto_vra.get_response()

def insertVRATodo(API_CONFIG_FILE, objectId):
    payload = {'objectid': objectId}
    writeto_todo = atlasapiclient.WriteToToDo(api_config_file = API_CONFIG_FILE, payload=payload)
    writeto_todo.get_response()

def insertVRARank(API_CONFIG_FILE, objectId, rank, is_gal_cand):
    payload = {'objectid': objectId, 'rank': rank, 'is_gal_cand': is_gal_cand}
    writeto_rank = atlasapiclient.WriteToVRARank(api_config_file = API_CONFIG_FILE, payload=payload)
    writeto_rank.get_response()

def updateObjectDetectionList(API_CONFIG_FILE, objectId, objectList = 4):
    payload = {'objectid': objectId, 'objectlist': objectList}
    update_list = atlasapiclient.WriteObjectDetectionListNumber(api_config_file = API_CONFIG_FILE, payload=payload)
    update_list.get_response()

def runUpdates(options):
    """
    Read the VRA table on the following conditions:
      - timestamp is less than N days ago
      - debug = False
      - output the list of ATLAS 19 digit IDs and timestamps (in pairs).
    """

    debug = options.debug
    if debug is None:
        debug = False

    api_config = options.apiConfigFile
    rank_column = options.rankcolumn
    # NEW LOGIC: Get the VRA todo list, NOT use a date threshold anymore.
    #            NOTE: We may need to chunk the response we get when making up the payload below.
    #            OR: Put in a date threshold below of (e.g.) 1 month of data from VRA Todo. Then
    #            use the dataframe to make any further cuts.
    #            TODO: Decide what to set the date threshold to use.
    #            NOTE: Needs a change in the st3ph3n code which doesn't yet recognise the new VRATodoList.

    # Find the date from which we want to check for new information
    date_threshold = (datetime.now() - timedelta(days=float(options.ndays))).strftime("%Y-%m-%d %H:%M:%S")
    payload = {'datethreshold': date_threshold}

    request_vra_todolist = atlasapiclient.RequestVRAToDoList(api_config_file=api_config,  # Config file defined above
                                                       payload = payload,           # Payload to API just includes date threshold
                                                       get_response = True          # Query the server on instantiation.
                                                       )

    vratodo_df = pd.DataFrame(request_vra_todolist.response_data)                   # Use Pandas to handle cuts below witht having to use loops.
    #print(vratodo_df)


    # NEW LOGIC: Make the payload with the ATLAS IDs in the VRA todo.
    #            New API call to read the tcs_vra_todo list.

    # Setup the request VRA scores utility with the VRA token & username.
    request_vra_scores = atlasapiclient.RequestVRAScores(api_config_file=api_config,  # Config file defined above
                                                   payload = payload,           # Payload to API just includes date threshold
                                                   get_response = True          # Query the server on instantiation.
                                                   )

    vra_df = pd.DataFrame(request_vra_scores.response_data)                          # Use Pandas to handle cuts below witht having to use loops.

    # NEW LOGIC: Make a sub-dataframe that only contains ATLAS IDs in the todo list.
    last_vra_timestamps = vra_df[np.isin(vra_df.transient_object_id.values,     # Only select transient_object_ids of objects we want to update. Must be values - returns numpy array.
                                              vratodo_df.transient_object_id.values) 
                                ][['transient_object_id','timestamp']
                                 ].drop_duplicates(subset='transient_object_id',# Remove duplicate rows and only keep the last one
                                                   keep='last')
    last_vra_timestamps.set_index('transient_object_id', inplace=True)          # Use transient_object_id as index for convenience.

    ### NEW CODE HFS: 2024-08-12: ADDING THE BRAIN TO OUR UPDATE SCORES
    # HFS: add the mjd so can compare to the last observation in the lightcurve
    last_vra_timestamps['mjd'] = Time(pd.to_datetime(last_vra_timestamps.timestamp.values)).mjd

    atlas_id_tns_xm = []
    feature_list = []
    atlas_ids_to_update = []

    for _atlas_id in vratodo_df.transient_object_id.values:
        ## HFS: 2024-08-17 -- Adding Features to the Update Scorer
        feature_maker = FeaturesSingleSource(atlas_id=str(_atlas_id),
                                             api_config_file = api_config ,
                                             )
    
    
        # Keep a record of transients with TNS crossmatches.
        if len(feature_maker.json_data.data['tns_crossmatches']) > 0:
            atlas_id_tns_xm.append(_atlas_id)
    
        # IF LAST OBSERVATION WAS MORE THAN 1 DAY AGO WE RERUN THE FEATURES
        if (feature_maker.last_visit_mjd-last_vra_timestamps.loc[_atlas_id].mjd)>0:
    
            feature_maker.make_dayN_features()
            feature_list.append(feature_maker.dayN_features)
            ##
            atlas_ids_to_update.append(_atlas_id)
    
        else:
            continue


    ## HFS: 2024-08-17 -- Adding Features to the Update Scorer
    if not atlas_ids_to_update:
        if len(atlas_id_tns_xm) == 0:
            # We have nothing to do. No updates and nothing found by TNS.
            return 0
        else:
            # Set the rank to 10 for objects with a TNS crossmatch.
            for atlas_id in atlas_id_tns_xm:
                if rank_column != 'rank':
                    # If we are filling an alternative rank column we don't want
                    # to add the TNS crossmatch override row to the tcs_vra_scores
                    # table, as this will already be done by the scripts filling
                    # the main rank column. (We don't want duplicates.)
                    break
                rank = 10
                insertVRAEntry(api_config, atlas_id, None, None, rank, rank_column=rank_column, is_gal_cand = None, debug = debug)
                if options.debug:
                    continue
                else:
                    insertVRARank(api_config, atlas_id, rank, None)
            return 0


    features_df = pd.DataFrame(np.array(feature_list), columns = feature_maker.feature_names_dayN)
    ##
    s_a_r = ScoreAndRank(features_df,model_type='dayN', model_name='crabby')
    s_a_r.calculate_rank()

    gal_flags = np.array( ( s_a_r.is_gal_cand & (s_a_r.ranks<EYEBALL_THRESHOLD) ) ).astype(int) 

    i = 0
    for atlas_id, pReal, pGal, rank, gal_flag in zip(atlas_ids_to_update,
                                     s_a_r.real_scores.T[1],
                                     s_a_r.gal_scores.T[1],
                                     s_a_r.ranks,
                                     gal_flags):

        insertVRAEntry(api_config, atlas_id, pReal, pGal, rank, rank_column=rank_column, is_gal_cand = gal_flag, debug = debug)
                                       
        i += 1
        if options.debug:
            continue
        else:
            insertVRARank(api_config, atlas_id, rank, gal_flag)
            if gal_flag:
                updateObjectDetectionList(api_config, atlas_id, 12)
            else:
                # HFS 2024-09-25: 
                # when not in debug mode we make sure that the updated objects list is reset to eyeball list = 4
                updateObjectDetectionList(api_config, atlas_id, 4)

    # Set the rank to 10 for objects with a TNS crossmatch.
    for atlas_id in atlas_id_tns_xm:
        if rank_column != 'rank':
            # If we are filling an alternative rank column we don't want
            # to add the TNS crossmatch override row to the tcs_vra_scores
            # table, as this will already be done by the scripts filling
            # the main rank column. (We don't want duplicates.)
            break
        rank = 10
        insertVRAEntry(api_config, atlas_id, None, None, rank, rank_column=rank_column, is_gal_cand = gal_flag, debug = debug)
        if options.debug:
            continue
        else:
            insertVRARank(api_config, atlas_id, rank, gal_flag)

    # 6. Calculate ranks and for each atlas ID, rank pair write to the tcs_vra_rank table.


    #for row in data:
    #    #print(row['objectid'], row['score'])
    #    insertVRAEntry(apiURL, apiToken, row['objectid'], row['score'], debug = debug)

    print("%d objects inserted into the VRA Scores and Todo tables." % i)
    return 0




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
