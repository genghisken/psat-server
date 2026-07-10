#!/usr/bin/env python
"""Run the PyTorch triplet CNN on Pan-STARRS images (multiprocess).

Intended to live in /storage1/software/CNN_PANSTARRS/ alongside cnn_data.py.

Set PSAT_ML_PATH to the folder containing runKerasTensorflowClassifierOnPSATImages.py
when that module is not on PYTHONPATH.

Usage:
  %s <configFile> [<candidate>...] [--ps1classifier=<ps1classifier>] [--ps2classifier=<ps2classifier>] [--ps1legacyclassifier=<ps1legacyclassifier>] [--ps2legacyclassifier=<ps2legacyclassifier>] [--outputcsv=<outputcsv>] [--comparecsv=<comparecsv>] [--listid=<listid>] [--imageroot=<imageroot>] [--update] [--cnn_data_root=<cnn_data_root>] [--zscale_stats_ps1=<zscale_stats_ps1>] [--zscale_stats_ps2=<zscale_stats_ps2>] [--batch_size=<batch_size>] [--loglocation=<loglocation>] [--logprefix=<logprefix>] [--candidatesinfiles] [--trainer=<trainer>]
  %s (-h | --help)
  %s --version

Options:
  -h --help                               Show this screen.
  --version                               Show version.
  --listid=<listid>                       List ID [default: 4].
  --ps1classifier=<ps1classifier>         PS1 PyTorch model (.pt).
  --ps2classifier=<ps2classifier>         PS2 PyTorch model (.pt).
  --ps1legacyclassifier=<ps1legacyclassifier>
                                          Optional legacy Keras PS1 classifier (.h5) for comparison.
  --ps2legacyclassifier=<ps2legacyclassifier>
                                          Optional legacy Keras PS2 classifier (.h5) for comparison.
  --outputcsv=<outputcsv>                 Combined PyTorch scores CSV.
  --comparecsv=<comparecsv>               Combined comparison CSV when legacy classifiers are supplied.
  --imageroot=<imageroot>                 Root location of stamp images [default: /db4/images/].
  --update                                Update the database with PyTorch scores.
  --cnn_data_root=<cnn_data_root>         CNN data root for zscale_stats.json [default: /storage1/software/CNN_PANSTARRS/data].
  --zscale_stats_ps1=<zscale_stats_ps1>   Override PS1 z-scale stats JSON.
  --zscale_stats_ps2=<zscale_stats_ps2>   Override PS2 z-scale stats JSON.
  --batch_size=<batch_size>               Inference batch size [default: 32].
  --loglocation=<loglocation>             Log file location [default: /tmp/].
  --logprefix=<logprefix>                 Log prefix [default: ml_pytorch_].
  --candidatesinfiles                     Interpret inline candidate IDs as files.
  --trainer=<trainer>                     Legacy Keras trainer module [default: PSAT-D].

Example:
  python %s ~/config.yaml --ps1classifier=cnn_models/cnn_model_ps1.pt --listid=4 --outputcsv=/tmp/ps1_pytorch_list_4.csv --update
  python %s ~/config.yaml --ps1classifier=/home/panstarrs/machine_learning/classifiers/pytorchcnn/cnn_models/cnn_model_ps1.pt --ps2classifier=/home/panstarrs/machine_learning/classifiers/pytorchcnn/cnn_models/cnn_model_ps2.pt --listid=1 --outputcsv=/tmp/pytorch_scores.csv --imageroot=/astrosurveydb2/images/
"""
import csv
import datetime
import glob
import os
import sys

__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0])

_psat_ml = os.environ.get("PSAT_ML_PATH")
if _psat_ml and _psat_ml not in sys.path:
    sys.path.insert(0, _psat_ml)

from docopt import docopt
from gkutils.commonutils import Struct, cleanOptions, dbConnect, parallelProcess, splitList

from pytorch_utils import (
    getObjectsByList,
    updateTransientRBValue,
)

from runPytorchTripletClassifierOnPSATImages import runPytorchTripletClassifier


def worker(num, db, objectListFragment, dateAndTime, firstPass, miscParameters, q):
    """Process worker: score a fragment of candidates and return results via queue."""
    options = miscParameters[0]
    sys.stdout = open(
        "%s%s_%s_%d.log" % (options.loglocation, options.logprefix, dateAndTime, num),
        "w",
    )

    options.candidate = [str(row["id"]) for row in objectListFragment]
    options.candidatesinfiles = None

    objects_for_update = runPytorchTripletClassifier(options, processNumber=num)

    print("Adding %d objects onto the queue." % len(objects_for_update))
    q.put(objects_for_update)
    print("Process complete.")
    print("DB Connection Closed - exiting")
    return 0


def _merge_compare_csv(compare_path, compare_glob_pattern):
    """Merge per-worker comparison CSV shards into one file."""
    worker_paths = sorted(glob.glob(compare_glob_pattern))
    if not worker_paths:
        return

    merged = {}
    for worker_path in worker_paths:
        for object_id, pytorch_score, keras_score in _read_worker_compare_csv(worker_path):
            merged[object_id] = (pytorch_score, keras_score)

    with open(compare_path, "w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["object_id", "pytorch_score", "keras_score"])
        for object_id in sorted(merged.keys(), key=lambda value: int(value)):
            pytorch_score, keras_score = merged[object_id]
            writer.writerow([object_id, pytorch_score, keras_score])


def _read_worker_compare_csv(path):
    rows = []
    with open(path, newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            rows.append(
                (
                    row["object_id"],
                    row["pytorch_score"],
                    row["keras_score"],
                )
            )
    return rows


def runPytorchTripletClassifierMultiprocess(opts):
    if type(opts) is dict:
        options = Struct(**opts)
    else:
        options = opts

    import yaml

    with open(options.configFile) as yaml_file:
        config = yaml.safe_load(yaml_file)

    username = config["databases"]["local"]["username"]
    password = config["databases"]["local"]["password"]
    database = config["databases"]["local"]["database"]
    hostname = config["databases"]["local"]["hostname"]

    db = []

    conn = dbConnect(hostname, username, password, database)
    if not conn:
        print("Cannot connect to the database")
        return 1

    conn.autocommit(True)

    ps1_data = bool(
        options.ps1classifier
        or options.ps2classifier
        or options.ps1legacyclassifier
        or options.ps2legacyclassifier
    )
    if not ps1_data:
        print("PyTorch triplet CNN currently supports Pan-STARRS PS1/PS2 only.")
        conn.close()
        return 1

    if options.listid is not None:
        try:
            detection_list = int(options.listid)
            if detection_list < 0 or detection_list > 8:
                print("Detection list must be between 0 and 8")
                return 1
        except ValueError:
            sys.exit("Detection list must be an integer")

    object_list = []
    if len(options.candidate) > 0:
        if options.candidatesinfiles:
            candidates = []
            for candidate_file in options.candidate:
                with open(candidate_file) as fp:
                    content = fp.readlines()
                candidates += [line.strip() for line in content]
            object_list = [{"id": int(candidate)} for candidate in candidates]
        else:
            object_list = [{"id": int(candidate)} for candidate in options.candidate]
    else:
        object_list = getObjectsByList(conn, database, listId=int(options.listid), ps1Data=True)

    if len(object_list) > 500:
        _, sub_lists = splitList(object_list, bins=16)
    else:
        sub_lists = [object_list]

    all_objects_for_update = []

    for sub_list in sub_lists:
        current_date = datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")
        (year, month, day, hour, minute, sec) = current_date.split(":")
        date_and_time = "%s%s%s_%s%s%s" % (year, month, day, hour, minute, sec)

        objects_for_update = []

        if len(object_list) > 0:
            n_processors, list_chunks = splitList(sub_list, bins=64)

            print("%s Parallel Processing..." % datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S"))
            objects_for_update = parallelProcess(
                db,
                date_and_time,
                n_processors,
                list_chunks,
                worker,
                miscParameters=[options],
            )
            print("%s Done Parallel Processing" % datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S"))
            print("TOTAL OBJECTS TO UPDATE = %d" % len(objects_for_update))

            objects_for_update = sorted(objects_for_update, key=lambda row: row[1])
            all_objects_for_update.extend(objects_for_update)

            if options.outputcsv is not None and objects_for_update:
                with open(options.outputcsv, "w") as handle:
                    for row in objects_for_update:
                        print(row[0], row[1])
                        handle.write("%s,%f\n" % (row[0], row[1]))

            if options.comparecsv and (options.ps1legacyclassifier or options.ps2legacyclassifier):
                prefix = options.comparecsv.rsplit(".", 1)[0]
                suffix = options.comparecsv.rsplit(".", 1)[-1] if "." in options.comparecsv else "csv"
                compare_glob = "%s_*.%s" % (prefix, suffix)
                _merge_compare_csv(options.comparecsv, compare_glob)
                print("Wrote combined comparison CSV:", options.comparecsv)

            if options.update:
                for row in objects_for_update:
                    updateTransientRBValue(conn, row[0], row[1], ps1Data=True)

    conn.close()
    return all_objects_for_update


def main():
    opts = docopt(__doc__, version="0.1")
    opts = cleanOptions(opts)
    options = Struct(**opts)
    runPytorchTripletClassifierMultiprocess(options)


if __name__ == "__main__":
    main()
