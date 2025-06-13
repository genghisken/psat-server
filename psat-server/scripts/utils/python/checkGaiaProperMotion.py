#!/usr/bin/env python3
"""
Script to crossmatch sherlock catalog objects with local Gaia (accounting for proper motion
and uncertainties) and (optionally) download & plot target, reference, and difference images with overlays:
  - A red circle marking the Gaia source's original 2016 position,
  - A green circle marking the Gaia position at the time of observation (propagated),
  - A magenta circle marking the Gaia position propagated to 2008,
  - A violet circle marking the centre of the cutout (observed transient),
  - Yellow dashed lines connecting magenta-red and red-green,
  - And a cyan uncertainty ellipse (around the 2016 position).
Additionally, a distribution plot of source match separations is produced.

Usage:
  %s (-h | --help)
  %s <config_file> [<candidate>...]
            [--list=<listid>] [--customlist=<customlistid>]
            [--catalog_file=<catalog_file>]
            [--mode=<mode>]
            [--do_plot]
            [--output_folder=<output_folder>] [--output_csv=<output_csv>]
            [--ra_column=<ra_column>] [--dec_column=<dec_column>] [--mjd_column=<mjd_column>] [--id_column=<id_column>]
            [--date=<date>] [--survey=<survey>]
            [--loglocation=<loglocation>] [--logprefix=<logprefix>]
            [--pixelscaleps=<pixelscaleps>] [--pixelscaleat=<pixelscaleat>]
            [--radius=<radius>] [--maxpm=<maxpm>] [--maxsep=<maxsep>]
            [--baseurl=<baseurl>]
            [--update]
            [--usestampmjd]
            [--overrideflags]


Options:
  -h --help                     Show this help message and exit.
  --list=<listid>               The object list, ignored if candidate or catalog_file provided
  --customlist=<customlistid>   The object custom list, ignored if candidate or catalog_file provided
  --catalog_file=<catalog>      Tab-separated catalog file. Overrides connection to the database.
  --output_folder=<folder>      Directory to save plots.
  --output_csv=<csv>            Optional CSV for matched sources [default: matches.csv].
  --mode=<mode>                 Operation mode: default or all [default: default].
  --do_plot                     Download & plot cutouts for each match.
  --ra_column=<ra>              RA column for all mode [default: ra]
  --dec_column=<dec>            DEC column for all mode [default: dec]
  --mjd_column=<mjd>            MJD column for all mode [default: mjd].
  --id_column=<id>              ID column for all mode [default: id].
  --date=<date>                 Date threshold - no hyphens. If date is a small number assume number of days before NOW [default: 20100101]
  --survey=<survey>             Survey database to interrogate [default: atlas].
  --numberOfThreads=<n>         Number of threads (stops external database overloading) [default: 10]
  --loglocation=<loglocation>   Log file location [default: /tmp/]
  --logprefix=<logprefix>       Log prefix [default: gaia_pm_crossmatch]
  --pixelscaleps=<pixelscaleps>   Pan-STARRS pixel scale [default: 0.25]
  --pixelscaleat=<pixelscaleat>   ATLAS pixel scale [default: 1.86]
  --radius=<radius>             Base search radius (arcsec) [default: 5.0]
  --maxpm=<maxpm>               Maximum proper motion in arcsec per year [default: 5.0]
  --maxsep=<maxsep>             Maximum separation allowed [default: 4.0]
  --baseurl=<baseurl>           Webserver URL (for finding stamps externally) [default: https://star.pst.qub.ac.uk/sne/]
  --update                      Update the database
  --usestampmjd                 Override the mjd in the data with the one extracted from the stamp filename.
  --overrideflags               Override the processing (pmcheck) flag.


  E.g.:
    %s config.yaml --catalog_file=test.tst --output_folder=/tmp/ --output_csv=test.csv --do_plot
    %s config.yaml --catalog_file=test.tst --output_folder=/tmp/ --mode=all --ra_column=ra_psf --dec_column=dec_psf --id_column=stamp_id --output_csv=output.csv --do_plot
    %s config.yaml 1131057501112826700 --output_folder=/tmp/ --mode=all --ra_column=ra_psf --dec_column=dec_psf --id_column=stamp_id --output_csv=output.csv --do_plot --mjd_column=mjd_obs --id_column=id --radius=5.0 --update

"""

import os
import sys
import logging
import time
import math

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")  # For headless environments
import matplotlib.pyplot as plt
from astropy.io import fits
from astropy.visualization import ZScaleInterval
from astropy.coordinates import SkyCoord, Distance
from astropy.time import Time
from astropy import units as u
from matplotlib.patches import Ellipse
import gc

from urllib.request import urlretrieve

from docopt import docopt
from gkutils.commonutils import dbConnect, splitList, parallelProcess
__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0])

from docopt import docopt
import sys, os, shutil, re, csv
from gkutils.commonutils import dbConnect, Struct, cleanOptions, PROCESSING_FLAGS

# TEMPORARY CODE until gkutils is updated: New proper motion check flag.
PROCESSING_FLAGS['pmcheck'] = 0x2000

sys.path.append('../../common/python')
from queries import getATLASCandidates, getPanSTARRSCandidates, updateTransientObservationAndProcessingStatus, insertTransientObjectComment

# ------------------------
# Global constants and setup
# ------------------------
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s",
                    stream=sys.stdout)

PIXEL_SCALE_PS = 0.25  # Pan-STARRS pixel scale. cutouts are 200x200, so centre is (100,100)
PIXEL_SCALE_ATLAS = 1.862 # ATLAS ACAM pixel scale

# ------------------------
# Tiny Struct for docopt results
# ------------------------
#class Struct:
#    """Turn a dict into an object: access keys as attributes."""
#    def __init__(self, **entries):
#        self.__dict__.update(entries)

def parse_args():
    opts = docopt(__doc__, version='0.1')
    opts = cleanOptions(opts)
    options = Struct(**opts)

    return options
#    return Struct(
#        catalog_file   = opts["--catalog_file"],
#        output_folder  = opts["--output_folder"],
#        output_csv     = opts.get("--output_csv") or opts["--output_csv"],
#        do_plot        = opts["--do_plot"],
#        mode           = opts["--mode"],
#        ra_column      = opts.get("--ra_column"),
#        dec_column     = opts.get("--dec_column"),
#        mjd_column     = opts.get("--mjd_column"),
#        id_column      = opts.get("--id_column")
#    )

# ------------------------
# Placeholder for local DB connection
# ------------------------
def get_local_connection():
    """
    Return a connection object for your local Gaia database.
    Replace this with your actual connection routine.
    """
    import MySQLdb
    hostname = 'db6'
    username = 'kws'
    password = ''
    database = 'crossmatch_catalogues'
    conn = dbConnect(hostname, username, password, database, quitOnError=True)
    if conn is None:
        print("PANIC")
    return conn

# ------------------------
# Local Gaia proper motion crossmatch function
# ------------------------
def crossmatchWithLocalGaiaProperMotion(conn, options, objectRow):
    """
    Crossmatch with the local Gaia database while accounting for proper motion.
    Returns (searchDone, best_match_dict_or_None).
    """
    GAIA_REF_EPOCH = 2016.0

    ra = objectRow['ra']
    dec = objectRow['dec']
    mjd = objectRow.get('mjd')
    print(ra, dec, mjd)

    if mjd is None:
        print("Missing 'mjd' in objectRow for proper motion query.")
        return False, None

    obs_time = Time(mjd, format='mjd')
    observation_epoch = obs_time.jyear
    dt = abs(observation_epoch - GAIA_REF_EPOCH)
    expanded_radius = float(options.radius) + float(options.maxpm) * dt

    from gkutils.commonutils import coneSearchHTM, FULL
    message, matches = coneSearchHTM(ra, dec, expanded_radius, 'tcs_cat_gaia_dr3', queryType=FULL, conn=conn)
    print("Got matches", len(matches))
    if message and (message.startswith('Error') or 'not recognised' in message):
        print("Database error in Gaia proper motion query: ", message)
        return False, None
    if not matches:
        return True, None

    target_coord = SkyCoord(ra=ra*u.deg, dec=dec*u.deg, frame='icrs')
    best_match = None
    min_sep = np.inf

    for separation_initial, row in matches:
        galaxy = row.get('classprob_dsc_combmod_galaxy')
        qso    = row.get('classprob_dsc_combmod_quasar')
        if galaxy is not None and qso is not None and (galaxy + qso) > 0.5:
            continue

        try:
            pmra     = row['pmra']
            pmdec    = row['pmdec']
            parallax = row['parallax']
        except KeyError:
            continue
        if parallax is None or parallax <= 0:
            continue

        distance = Distance(parallax=parallax*u.mas)
        gaia_coord = SkyCoord(
            ra=row['ra']*u.deg, dec=row['dec']*u.deg,
            distance=distance,
            pm_ra_cosdec=pmra*u.mas/u.yr,
            pm_dec=pmdec*u.mas/u.yr,
            obstime=Time(GAIA_REF_EPOCH, format='jyear'),
            frame='icrs'
        )
        gaia_coord_at_obs = gaia_coord.apply_space_motion(new_obstime=obs_time)
        separation = target_coord.separation(gaia_coord_at_obs).arcsecond

        if separation < min_sep and separation <= float(options.maxsep):
            min_sep = separation
            best_match = {
                'source_id':    row.get('source_id'),
                'ra_gaia':      row['ra'],
                'dec_gaia':     row['dec'],
                'pmra':         pmra,
                'pmdec':        pmdec,
                'parallax':     parallax,
                'propagated_ra':   gaia_coord_at_obs.ra.deg,
                'propagated_dec':  gaia_coord_at_obs.dec.deg,
                'separation':     separation,
                # optional uncertainty fields if present:
                'eff_ra_err':    row.get('pmra_error'),
                'eff_dec_err':   row.get('pmdec_error'),
            }

    return True, best_match

# ------------------------
# Image download and plotting functions 
# ------------------------
def get_images_for_stamp(options, detection_stamp):
    """Helper function to download FITS cutouts and return 200×200 arrays."""
    def extract_folder(stamp):
        parts = stamp.split('_')
        return parts[1].split('.')[0] if len(parts) >= 2 else None

    def attempt_download(url, path, retries=3, delay=5):
        for attempt in range(retries):
            try:
                urlretrieve(url, path)
                return True
            except Exception as e:
                logging.info(f"Failed to download {url}: {e}. "
                             f"Attempt {attempt+1}/{retries}; retry in {delay}s.")
                time.sleep(delay)
        return False

    def crop_center(img, size=200):
        h, w = img.shape
        sx = w//2 - size//2
        sy = h//2 - size//2
        return img[sy:sy+size, sx:sx+size]

    folder = extract_folder(detection_stamp)
    if not folder:
        return np.zeros((200,200)), np.zeros((200,200)), np.zeros((200,200))

    if options.survey == 'atlas':
        BASE_URL = options.baseurl + 'atlas4/media/images/data/atlas4'
    elif options.survey == 'panstarrs':
        BASE_URL = options.baseurl + 'ps13pi/media/images/data/ps13pi'
    else:
        logging.error("Base URL is undefined")
        exit(1)

    urls = {
        'target': f"{BASE_URL}/{folder}/{detection_stamp}_target.fits",
        'ref':    f"{BASE_URL}/{folder}/{detection_stamp}_ref.fits",
        'diff':   f"{BASE_URL}/{folder}/{detection_stamp}_diff.fits"
    }
    files = {k: f"{detection_stamp}_{k}.fits" for k in urls}

    for k in urls:
        if not attempt_download(urls[k], files[k]):
            for fn in files.values():
                if os.path.exists(fn):
                    os.remove(fn)
            return np.zeros((200,200)), np.zeros((200,200)), np.zeros((200,200))

    try:
        data = {k: fits.getdata(files[k]) for k in files}
    except Exception as e:
        logging.info(f"Error reading FITS for {detection_stamp}: {e}")
        for fn in files.values():
            if os.path.exists(fn):
                os.remove(fn)
        return np.zeros((200,200)), np.zeros((200,200)), np.zeros((200,200))

    for fn in files.values():
        try: os.remove(fn)
        except Exception: pass

    return (crop_center(data['target']),
            crop_center(data['ref']),
            crop_center(data['diff']))

def plot_all_images_with_overlay(options, detection_stamp, best_match,
                                 object_ra, object_dec, output_file):
    """
    Downloads the target, ref, and diff images for detection_stamp and plots them side-by-side.
    Overlays colored circles and connecting lines per spec.
    """
    target_img, ref_img, diff_img = get_images_for_stamp(options, detection_stamp)
    if target_img.size == 0:
        logging.info(f"Could not get complete images for {detection_stamp}.")
        return

    pixelscale = 0.25 # default to Pan-STARRS pixel scale
    if options.survey == 'atlas':
        pixelscale = float(options.pixelscaleat)
    elif options.survey == 'panstarrs':
        pixelscale = float(options.pixelscaleps)

    # compute pixel offsets
    centre = (100, 100)
    def sky_to_pix(ra1, dec1, ra2, dec2):
        dra = (ra2 - ra1) * math.cos(math.radians(dec1)) * 3600.0
        ddec = (dec2 - dec1) * 3600.0
        return centre[0] - dra/pixelscale, centre[1] + ddec/pixelscale

    red_x, red_y   = sky_to_pix(object_ra, object_dec,
                                 best_match['ra_gaia'], best_match['dec_gaia'])
    green_x, green_y = sky_to_pix(object_ra, object_dec,
                                   best_match['propagated_ra'], best_match['propagated_dec'])

    # compute 2008 position
    gaia2016 = SkyCoord(
        ra=best_match['ra_gaia']*u.deg, dec=best_match['dec_gaia']*u.deg,
        pm_ra_cosdec=best_match['pmra']*u.mas/u.yr,
        pm_dec=best_match['pmdec']*u.mas/u.yr,
        obstime=Time(2016.0, format='jyear'),
        frame='icrs'
    )
    gaia2008 = gaia2016.apply_space_motion(new_obstime=Time(2008.0, format='jyear'))
    blue_x, blue_y = sky_to_pix(object_ra, object_dec,
                                 gaia2008.ra.deg, gaia2008.dec.deg)
    violet_x, violet_y = centre

    zscale = ZScaleInterval(contrast=0.1)
    fig, axes = plt.subplots(1, 3, figsize=(18, 6))
    for ax, img, title in zip(axes,
                              [target_img, ref_img, diff_img],
                              ["Target", "Reference", "Difference"]):
        vmin, vmax = zscale.get_limits(img)
        ax.imshow(img, cmap='gray', origin='lower', vmin=vmin, vmax=vmax)
        ax.set_title(title)
        ax.plot([violet_x, red_x],   [violet_y, red_y],   linestyle='--', lw=2)
        ax.plot([red_x, green_x],   [red_y, green_y],   linestyle='--', lw=2)
        ax.plot(green_x, green_y, marker='o', markersize=4, color='green', label="Gaia Obs")
        ax.plot(red_x,   red_y,   marker='o', markersize=4, color='red',   label="Gaia 2016")
        ax.plot(blue_x,  blue_y,  marker='o', markersize=4, color='magenta', label="Gaia 2008")
        ax.plot(violet_x, violet_y, marker='o', markersize=4, color='blue', label="Source")

        # uncertainty ellipse around 2016
        if best_match.get('eff_ra_err') and best_match.get('eff_dec_err'):
            w = 2 * best_match['eff_ra_err'] / pixelscale
            h = 2 * best_match['eff_dec_err'] / pixelscale
            ellipse = Ellipse((red_x, red_y), width=w, height=h,
                              edgecolor='cyan', facecolor='none', lw=1)
            ax.add_patch(ellipse)

        ax.axis('off')
        ax.legend(loc='upper right', fontsize=8)

    fig.suptitle(detection_stamp, fontsize=16)
    plt.tight_layout(rect=[0,0.03,1,0.95])
    plt.savefig(output_file, dpi=150)
    plt.close()
    logging.info(f"Saved plot for {detection_stamp} to {output_file}")

# ------------------------
# Worker Function for Parallel Processing
# ------------------------
def worker(i, db, chunk, dateAndTime, firstPass, miscParameters, q):
    """
    Process a chunk (list) of catalog rows in parallel.
    """
    import warnings
    from erfa.core import ErfaWarning

    # ignore all ErfaWarning warnings
    warnings.filterwarnings('ignore', category=ErfaWarning)

    options = miscParameters[0]

    id_field  = miscParameters[1].get("id_field", "detection_stamp")
    ra_col    = miscParameters[1].get("ra_column", "ra_psf")
    dec_col   = miscParameters[1].get("dec_column", "dec_psf")
    mjd_col   = miscParameters[1].get("mjd_column", "mjd")

    sys.stdout = open('%s%s_%s_%d.log' % (options.loglocation, options.logprefix, dateAndTime, i), "w")

#    conn = get_local_connection()
    conn = None
    try:
        conn = dbConnect(db['hostname'], db['username'], db['password'], db['database'], quitOnError = True)
        conn.autocommit(True)
    except:
        sys.stderr.write("Cannot connect to the local database. Terminating this process.\n")
        q.put([])
        return 0

    connCatalogues = None
    try:
        connCatalogues = dbConnect(db['cathost'], db['catuser'], db['catpass'], db['catname'], quitOnError = True)
    except:
        print("Cannot connect to the catalogues database. Terminating this process.")
        q.put([])
        return 0

    results = []

    for row in chunk:
        objectRow = {
            'ra':  row[ra_col],
            'dec': row[dec_col],
            'mjd': row[mjd_col]
        }
        searchDone, best_match = crossmatchWithLocalGaiaProperMotion(connCatalogues, options, objectRow)
        stamp = row.get(id_field, f"row_{i}")
        print("BEST_MATCH", best_match, searchDone)
        results.append((stamp, best_match, row))

    if conn:
        conn.close()
    if connCatalogues:
        connCatalogues.close()
    
    q.put(results)

# ------------------------
# Main processing function
# ------------------------
def main(options, catalog_file, output_csv, output_folder, do_plot,
         mode, ra_column, dec_column, mjd_column, id_column):

    import yaml
    with open(options.config_file) as yaml_file:
        config = yaml.safe_load(yaml_file)

    username = config['databases']['local']['username']
    password = config['databases']['local']['password']
    database = config['databases']['local']['database']
    hostname = config['databases']['local']['hostname']

    candidateList = []

    catuser = config['databases']['sherlock']['username']
    catpass = config['databases']['sherlock']['password']
    catname = config['databases']['sherlock']['database']
    cathost = config['databases']['sherlock']['hostname']


    db = {'username': username,
          'password': password,
          'database': database,
          'hostname': hostname,
          'catuser': catuser,
          'catpass': catpass,
          'catname': catname,
          'cathost': cathost}


    # Uncomment this database connection if we decide we want to do the updates AFTER the crossmatch.
    conn = dbConnect(hostname, username, password, database, quitOnError = True)

    # 2023-03-25 KWS MySQLdb disables autocommit by default. Switch it on globally.
    conn.autocommit(True)


    matches_data = []
    data = []
    chunk_size = 1_000_000

    processingFlags = PROCESSING_FLAGS['pmcheck']
    if options.overrideflags:
        processingFlags = 0

    if catalog_file:
        # read as an iterator of DataFrames
        data_iter = pd.read_csv(catalog_file, sep="\t", chunksize=chunk_size)
    elif options.candidate or options.list or options.customList:
        # pull from DB into a list of dicts
        if options.survey == 'atlas':
            data = getATLASCandidates(conn, options, processingFlags = processingFlags)
        elif options.survey == 'panstarrs':
            data = getPanSTARRSCandidates(conn, options, processingFlags = processingFlags)
        else:
            logging.error(f"Unsupported survey: {options.survey}")
            sys.exit(1)

        if not data:
            logging.error("No data returned from database!")
            sys.exit(1)

        # wrap into a list so our loop still works
        data_iter = [pd.DataFrame(data)]
    else:
        logging.error(
            "No input specified. "
            "Either pass --catalog_file or specify a candidate/list/customList."
        )
        sys.exit(1)







    for df in data_iter:
        if not hasattr(df, 'columns'):
            logging.error(f"Expected DataFrame, got {type(df)}")
            sys.exit(1)

        # determine columns per mode
        if mode == "default":
            if 'detection_stamp' not in df.columns:
                if 'stamp' in df.columns:
                    df.rename(columns={'stamp': 'detection_stamp'}, inplace=True)
                else:
                    logging.info("Neither 'detection_stamp' nor 'stamp' found.")
                    sys.exit(1)
            if 'mjd' not in df.columns or (df['detection_stamp'] is not None and options.usestampmjd):
                df['mjd'] = df['detection_stamp'].apply(
                    lambda s: float(s.split('_')[1]) if '_' in s else np.nan
                )
            id_field  = "detection_stamp"
            ra_field  = "ra_psf"
            dec_field = "dec_psf"
            mjd_field = "mjd"
        elif mode == "all":
            id_field = id_column if id_column in df.columns else "row_id"
            if id_field == "row_id":
                df[id_field] = df.index.astype(str)
            ra_field  = ra_column
            dec_field = dec_column
            mjd_field = mjd_column
        else:
            logging.info("Invalid mode selected.")
            sys.exit(1)

            
        object_rows = df.to_dict('records')
        df = None; gc.collect()

        miscParams = {
            "id_field":   id_field,
            "ra_column":  ra_field,
            "dec_column": dec_field,
            "mjd_column": mjd_field
        }
        nproc, chunks = splitList(object_rows, bins=128, preserveOrder=False)
        object_rows = None; gc.collect()

        dateAndTime = time.strftime("%Y-%m-%d %H:%M:%S")
        results_flat = parallelProcess(
            db=db,
            dateAndTime=dateAndTime,
            nProcessors=nproc,
            listChunks=chunks,
            worker=worker,
            miscParameters=[cfg, miscParams]
        )

        #print(results_flat)
        for stamp, best_match, orig in results_flat:
            if best_match is not None:
                matches_data.append({
                    id_field: stamp,
                    ra_field: orig[ra_field],
                    dec_field: orig[dec_field],
                    mjd_field: orig[mjd_field],
                    'source_match_distance': best_match['separation'],
                    'source_id': best_match['source_id']
                })
                
                if do_plot:
                    out_file = os.path.join(output_folder, f"{stamp}_gaia_match.png")
                    plot_all_images_with_overlay(
                        options,
                        stamp, best_match,
                        orig[ra_field], orig[dec_field],
                        out_file
                    )

    # write CSV if requested
    if output_csv:
        pd.DataFrame(matches_data).to_csv(output_csv, index=False)
        logging.info(f"Saved match catalog to {output_csv}")

    if options.update and (options.candidate or options.list or options.customList) and options.id_column == 'id':
        # We must be talking to the database, not an input CSV file. So do we
        # want to update the database?
        # Note that for the time being we are doing this single-threaded at the end of the PM check.
        # In future we might want to do the updates in parallel. This may or may not result in table
        # locking, but should be a lot quicker than doing single threaded.

        for row in data:
            # 1. Set the processing flag for all the data we just checked.
            if row[id_field] not in matches_data:
                rowsChecked = updateTransientObservationAndProcessingStatus(conn, row[id_field], processingFlag = PROCESSING_FLAGS['pmcheck'], survey = options.survey)

        for row in matches_data:
            # 2. Update the processing flag AND set observation_status for any objects that MATCH
            rowsUpdated = updateTransientObservationAndProcessingStatus(conn, row[id_field], processingFlag = PROCESSING_FLAGS['pmcheck'], observationStatus = 'hpmstar', survey = options.survey)

            # 3. Write a comment into the object comments table.
            comment = "HPM Check. Object is %.2f arcsec from Gaia DR3 source %s." % (row['source_match_distance'],row['source_id'])
            commentRowsUpdated = insertTransientObjectComment(conn, row[id_field], comment)


    if conn is not None:
        conn.close ()

# ------------------------
# Entrypoint
# ------------------------
if __name__ == "__main__":
    cfg = parse_args()
    print(cfg.catalog_file)
    print(cfg.output_folder)
    print(cfg.ra_column)
    os.makedirs(cfg.output_folder, exist_ok=True)

    main(
        cfg,
        cfg.catalog_file,
        cfg.output_csv,
        cfg.output_folder,
        cfg.do_plot,
        cfg.mode,
        cfg.ra_column,
        cfg.dec_column,
        cfg.mjd_column,
        cfg.id_column
    )

