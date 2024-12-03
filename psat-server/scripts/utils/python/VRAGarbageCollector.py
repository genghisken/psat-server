#!/usr/bin/env python
"""VRA garbage collector

Usage:
  %s <apiConfigFile> <outputFile> [--debug] 
  %s (-h | --help)
  %s --version

Options:
  -h --help                         Show this screen.
  --version                         Show version.
  --debug                           Debug mode.
  --rbthreshold=<rbthreshold>       RB Threshold (if not set will be ignored).

E.g.:
  %s ../../../../../atlas/config/api_config_file.yaml /tmp/object_to_delete.csv
"""

import sys
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0])
from docopt import docopt
import os, shutil, re, csv, subprocess
from gkutils.commonutils import Struct, cleanOptions, dbConnect, coords_dec_to_sex, getDateFractionMJD, readGenericDataFile

import requests
import json

import pandas as pd
import numpy as np


# 2024-06-24 KWS Use the st3ph3n API code to write the results.
from st3ph3n.utils import api as vraapi
from atlasapiclient import client as atlasapiclient

def insertVRAEntry(API_CONFIG_FILE, objectId, pReal, pGal, rank, debug = False):
    payload = {'objectid': objectId, 'preal': pReal, 'pgal': pGal, 'rank': rank, 'debug': debug}
    writeto_vra = atlasapiclient.WriteToVRAScores(api_config_file = API_CONFIG_FILE, payload=payload)
    writeto_vra.get_response()
    
    
def get_vra_eyeball():
    todo_list = atlasapiclient.RequestVRAToDoList(payload = {'datethreshold': "2024-02-22"}, get_response=True)
    todo_df=pd.DataFrame(todo_list.response)
    # TODO 2024-12-03 from atlasvras.utils.misc import fetch_vra_dataframe
    vra_df = vraapi.fetch_vra_dataframe(datethreshold=todo_df.timestamp.min()).set_index('transient_object_id')
    return vra_df.loc[todo_df.transient_object_id.values]


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
    outputFile = options.outputFile
    
    vra_eyeball_allrows = get_vra_eyeball()
    vra_df_eyeball= vra_eyeball_allrows.reset_index().drop_duplicates('transient_object_id', keep='last')


    # The delete conditions are based on the maximum VRA rank, mean VRA rank 
    # and how many visits an object has received
    max_ranks = vra_eyeball_allrows.reset_index().groupby('transient_object_id')['rank'].max()
    mean_ranks = vra_eyeball_allrows.reset_index().groupby('transient_object_id')['rank'].mean()
    counts = vra_eyeball_allrows.reset_index().groupby('transient_object_id').count().id.rename('counts')
    counts_n_ranks = pd.concat((max_ranks, mean_ranks, counts), axis=1)
    counts_n_ranks.columns = ['max_rank', 'mean_rank', 'counts']

    # The first condition is:
    # If an object has had at eadt one update and it's ranks never rose asbove 3
    ids_to_del_1 = set(counts_n_ranks[ (counts_n_ranks.counts>1)
                   &(counts_n_ranks['max_rank']<3)
                  ].index)
    # The second condition is 
    # If an oject was given a rank less than 1.5 delete (even if seen only once)
    ids_to_del_2 = set(counts_n_ranks[ (counts_n_ranks.counts>=1)
                   &(counts_n_ranks['max_rank']<1.5)
                  ].index) 
    # The third condition is
    # If an object has been camping for 3 days or more in the eyeball list but mean rank < 3: delete
    ids_to_del_3 = set(counts_n_ranks[ (counts_n_ranks.counts>=2)
                   &(counts_n_ranks['mean_rank']<3)
                  ].index) 


    list_to_delete = list(ids_to_del_1.union(ids_to_del_2).union(ids_to_del_3))

    delete_df = pd.DataFrame([list_to_delete]).T
    # save the atlas ids to delete to a text file
    delete_df.to_csv(outputFile, header=False, index=False)
    
    # add a row in the vra scores table to record the deletion
    for atlas_id in list_to_delete:
        insertVRAEntry(configFile, atlas_id, None, None, -1, debug = debug)
        
if __name__ == '__main__':
    main()
