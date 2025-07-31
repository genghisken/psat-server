"""
Code to make a better mookodi list

HERE BE WIZARDS - this is hacky AF because currently in ATLAS API Client the chunking
of the response SOMETIMES WORKS SOMETIMES DOES NOT. 

Right now it looks like with smaller chunks (25) it works when Requesting data
However it fails fails fails when trying to remove alerts from a list or add to a list. 

So for the READ operations, chunking works with small batches (?maybe?)
For WRITE operations chunking just doens't seem to work. 

For that reason I have EXTRA wrapper functions that call the WriteToCustomList
and RemoveFromCustomList classes and parse just one atlas id at a time. they also 
encapsulate some logging so we can keep track of what's going on. 
"""

import pandas as pd
import numpy as np
import atlasapiclient.client as ac
import logging


# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        #logging.FileHandler("atlas_debug.log"),
        logging.StreamHandler()
    ]
)

# ## Little hacks because chunking never works ##
#    Ken suspects that multiple objects in the payload just won't work because
#    Of the serialisers but I don't know why that would get Error 500s and not 400s

def add_single_atlas_id_to_list(atlas_id: int, list_name: str):
    try:
        logging.info(f"Adding {atlas_id} to '{list_name}'...")
        add_obj = ac.WriteToCustomList(array_ids=np.array([atlas_id]), 
                                       list_name=list_name)
        add_obj.get_response()
    except Exception as e:
        logging.exception("Error during WriteToCustomList call.")
        
def remove_single_atlas_id(atlas_id: int, list_name: str, api_config_file: str = None):
    try:
        logging.debug(f"Attempting to remove ATLAS ID {atlas_id} from list '{list_name}'")
        remover = ac.RemoveFromCustomList(
            array_ids=np.array([atlas_id]),
            list_name=list_name,
            api_config_file=api_config_file
        )
        remover.get_response()
    except Exception as e:
        logging.error(f"Failed to remove ATLAS ID {atlas_id} from '{list_name}': {e}")


###########################################################################
# Step 0: Remove targets from mookodi if the objects have been classified
try:
    logging.info("Fetching custom list (objectgroupid=16)...")
    mokoodi = ac.RequestCustomListsTable({'objectgroupid': 16}, get_response=True)
    mokoodi_list_df = pd.DataFrame(mokoodi.response_data).drop('object_group_id', axis=1)
    atlas_ids = mokoodi_list_df.transient_object_id.values.astype(str)
    logging.info(f"Fetched {len(atlas_ids)} entries from custom list.")
except Exception as e:
    logging.exception("Failed to fetch Mokoodi list.")
    raise

try:
    logging.info("Requesting source data in chunks...")
    multi_data = ac.RequestMultipleSourceData(
        array_ids=np.array(atlas_ids),
        mjdthreshold=60_500,
        chunk_size=25  # or 50, or whatever works
    )
    multi_data.chunk_get_response_quiet()
    logging.info(f"Received data for {len(multi_data.response_data)} sources.")
except Exception as e:
    logging.exception("Error fetching multiple source data.")
    raise
    
to_remove = []

for entry in multi_data.response_data:
    try:
        atlas_id = entry['object']['id']
        classification = entry['object'].get('observation_status')

        logging.debug(f"ATLAS ID {atlas_id} -> classification: {classification}" )
                      
        if classification is not None:
            to_remove.append(atlas_id)

    except Exception as e:
        logging.exception(f"Error processing source data entry.")
                      
for atlas_id in to_remove:
    remove_single_atlas_id(atlas_id, list_name='mookodi_live')

# #########################################################

# Step 1: Fetch list of ATLAS IDs In the old Mookodi list
try:
    logging.info("Fetching custom list (objectgroupid=2)...")
    mokoodi = ac.RequestCustomListsTable({'objectgroupid': 2}, get_response=True)
    mokoodi_list_df = pd.DataFrame(mokoodi.response_data).drop('object_group_id', axis=1)
    atlas_ids = mokoodi_list_df.transient_object_id.values.astype(str)
    logging.info(f"Fetched {len(atlas_ids)} entries from custom list.")
except Exception as e:
    logging.exception("Failed to fetch Mokoodi list.")
    raise

# Step 2: Request multiple source data in chunks
try:
    logging.info("Requesting source data in chunks...")
    multi_data = ac.RequestMultipleSourceData(
        array_ids=np.array(atlas_ids),
        mjdthreshold=60_500,
        chunk_size=25  # or 50, or whatever works
    )
    multi_data.chunk_get_response_quiet()
    logging.info(f"Received data for {len(multi_data.response_data)} sources.")
except Exception as e:
    logging.exception("Error fetching multiple source data.")
    raise

# Step 3: Process data and build good_targets / to_remove lists
good_targets = []
to_remove = []

for entry in multi_data.response_data:
    try:
        atlas_id = entry['object']['id']
        classification = entry['object'].get('observation_status')
        lc = entry.get('lc', [])

        if not lc:
            logging.debug(f"No lightcurve for {atlas_id}. Skipping.")
            continue

        last_mag = lc[-1].get('mag')
        if last_mag is None:
            logging.debug(f"No mag in last LC point for {atlas_id}. Skipping.")
            continue

        logging.debug(f"ATLAS ID {atlas_id} → classification: {classification}, last_mag: {last_mag}")

        if classification is None and last_mag < 17.0:
            good_targets.append(atlas_id)
        elif classification is not None:
            to_remove.append(atlas_id)

    except Exception as e:
        logging.exception(f"Error processing source data entry.")
       

# ADD GOOD TARGETS TO THE NEW LIST
for atlas_id in good_targets:
    add_single_atlas_id_to_list(atlas_id, list_name='mookodi_live')

    
# REMOVE BAD TARGETS FROM OLD LIST
for atlas_id in to_remove:
    remove_single_atlas_id(atlas_id, list_name='mookodi')
