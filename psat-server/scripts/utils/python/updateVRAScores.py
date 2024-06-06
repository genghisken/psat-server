#!/usr/bin/env python
"""Periodically update VRA scores as new data arrives and decision are made.

Usage:
  %s [--debug] [--ndays=<ndays>] [--quiet]
  %s (-h | --help)
  %s --version [--quiet]

Options:
  -h --help                         Show this screen.
  --version                         Show version.
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

    # NEW LOGIC: Get the VRA todo list, NOT use a date threshold anymore.
    #            NOTE: We may need to chunk the response we get when making up the payload below.
    #            OR: Put in a date threshold below of (e.g.) 1 month of data from VRA Todo. Then
    #            use the dataframe to make any further cuts.
    #            TODO: Decide what to set the date threshold to use.
    #            NOTE: Needs a change in the st3ph3n code which doesn't yet recognise the new VRATodoList.

    # Find the date from which we want to check for new information
    date_threshold = (datetime.now() - timedelta(days=float(options.ndays))).strftime("%Y-%m-%d %H:%M:%S")
    payload = {'datethreshold': date_threshold}

    request_vra_todolist = atlasapi.RequestVRAToDoList(api_config_file=api_config,  # Config file defined above
                                                       payload = payload,           # Payload to API just includes date threshold
                                                       get_response = True          # Query the server on instantiation.
                                                       )

    vratodo_df = pd.DataFrame(request_vra_todolist.response)                        # Use Pandas to handle cuts below witht having to use loops.
    #print(vratodo_df)


    # NEW LOGIC: Make the payload with the ATLAS IDs in the VRA todo.
    #            New API call to read the tcs_vra_todo list.

    # Setup the request VRA scores utility with the VRA token & username.
    request_vra_scores = atlasapi.RequestVRAScores(api_config_file=api_config,  # Config file defined above
                                                   payload = payload,           # Payload to API just includes date threshold
                                                   get_response = True          # Query the server on instantiation.
                                                   )

    vra_df = pd.DataFrame(request_vra_scores.response)                          # Use Pandas to handle cuts below witht having to use loops.

    # NEW LOGIC: Make a sub-dataframe that only contains ATLAS IDs in the todo list.

    list_to_update = vratodo_df.transient_object_id.values

    # Find the unique transient_object_ids of the objects that need updating.
    # NEW LOGIC: We already have a unique list of object IDs from the todo list.
    #            The following lines may cease to exist because human decisions
    #            already remove the ID from the todo list.
#    unique_transient_ids = set(vra_df.transient_object_id.values)               # All unique transient IDs in the VRA table for the last N days.
#    transient_ids_with_decisions = set(vra_df[~vra_df.username.isnull()         # Find unique IDs of objects with human decisions (username is not null)
#                                             ].transient_object_id.values)
#    set_to_update = unique_transient_ids - transient_ids_with_decisions         # Object to update are ONLY those WITHOUT a human decision.

    # Find the last time an object was updated in the VRA table.
    last_vra_timestamps = vra_df[np.isin(vra_df.transient_object_id.values,     # Only select transient_object_ids of objects we want to update. Must be values - returns numpy array.
                                              list_to_update) 
                                ][['transient_object_id','timestamp']
                                 ].drop_duplicates(subset='transient_object_id',# Remove duplicate rows and only keep the last one
                                                   keep='last')
    last_vra_timestamps.set_index('transient_object_id', inplace=True)          # Use transient_object_id as index for convenience.

    # Now go and fetch all the info pertaining to each of the objects above since our date threshold.
    # Instantiate the object that makes the API call, but don't actually do it yet.
    request_data = atlasapi.RequestMultipleSourceData(api_config_file=api_config,
                                                     array_ids=np.array(list_to_update),
                                                     mjdthreshold = getMJDFromSqlDate(date_threshold)
                                                     )

    # Chunks the API calls into payloads containing the max number of object that API can handle.
    if options.quiet:
        request_data.chunk_get_response_quiet()                                  # Quiet mode doen't print a progress bar.
    else:
        request_data.chunk_get_response()

    # Now iterate over each object we want to update.

    writeto_vra = atlasapi.WriteToVRAScores(api_config_file=api_config)         # Instantiate the WriteToVRAScores API connector. We only need to do this once.

    for i in range(len(request_data.response)):
        _response = request_data.response[i]                                                       # the response for one object
        data_lc = pd.DataFrame(_response['lc'])                                                    # store the det info in a pd dataframe
        data_lcnondets = pd.DataFrame(_response['lcnondets'])                                      # ditto for nondets

        # If statements to handle cases where there may not be any detections or nondetections
        if data_lc.shape[0] == 0:
            data_lc = None
        else:
            # do the operations on data_lc
            data_lc = data_lc[['mjd','filter','mag','magerr','date_inserted']]                         # Only select the columns we need from the det dataframe
            data_lc['det'] = 1                                                                         # Add new column to indicate that these are detections.


        if data_lcnondets.shape[0] == 0:
            data_lcnondets = None
        else:
            # do the operations on data_lcnondets
            data_lcnondets['magerr'] = 0.0                                                             # add a magerr column & set to zero for nondets
            data_lcnondets = data_lcnondets[['mjd','filter','mag5sig','magerr','date_inserted']]       # Select the columns we need for nondets.
            data_lcnondets.columns=['mjd','filter','mag','magerr','date_inserted']                     # Need to rename the columns so that we can concatenate later on (mag5sig -> mag)
                                                                                                       # Needed so that we can concatenate dataframes later on
            data_lcnondets['det'] = 0                                                                  # Add a new column to indicate that these are nondetections.



        #print(data_lcnondets.columns)
        #print(data_lcnondets)

        if data_lc is None and data_lcnondets is not None:
            data_all = data_lcnondets
        elif data_lcnondets is None and data_lc is not None:
            data_all = data_lc
        elif data_lcnondets is not None and data_lc is not None:
            data_all = pd.concat([data_lc, data_lcnondets]                                             # Concatenate dets and nondets to make full lightcurve
                                ).sort_values('mjd').reset_index(drop=True)                            # We also sort by mjd and reset the pandas index so that it's sequential and unique
        else:
            # There are neither detections nor non detections (both are None).
            # This shouldn't happen, but could in the situation that database has
            # been cleansed.
            continue

        #print(data_all.date_inserted.values)
        #print(last_vra_timestamps.loc[_response['object']['id']].values)

        # _response['object']['id'] is the unique ATLAS ID for this object.
        # We use it to locate the last time it was updated in the VRA table using last_vra_timestamps dataframe
        # We look for all the rows in our lightcurve (data_all) that are more recent than the last update.
        _transient_object_id = _response['object']['id']
        data_new = data_all[data_all.date_inserted.values > last_vra_timestamps.loc[_transient_object_id].values]

        if data_new.shape[0] == 0:
             continue                                                                              # If the dataframe is empty, move on to the next iteration.
        else:
             # TODO - possibly be smarter about case where there more than one night of new data available.
             mask_latest_night = (data_new.mjd.max() - data_new.mjd.values) < 1
             data_latest_night = data_new[mask_latest_night]

             # First bit of logic - if more than half data is detections, preal = 1, otherwise preal = 0
             _preal = data_latest_night.det.sum() // data_latest_night.shape[0]
             # Insert the VRA entry into the table.

             writeto_vra.payload = {'objectid': int(_transient_object_id),
                                    'preal': float(_preal),
                                    'debug': 1,
                                   }
             writeto_vra.get_response()




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
