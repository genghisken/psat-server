#!/usr/bin/env python
"""Run the PyTorch triplet CNN (target/ref/diff) on Pan-STARRS images.

Intended to live in /storage1/software/CNN_PANSTARRS/ alongside cnn_data.py.

Parallel to runKerasTensorflowClassifierOnPSATImages.py but loads all three stamp
planes with z-scale normalisation. Optionally run the legacy Keras diff-only
classifier on the same candidates for side-by-side comparison.

Set PSAT_ML_PATH to the folder containing runKerasTensorflowClassifierOnPSATImages.py
when using legacy comparison or if that module is not on PYTHONPATH.

Usage:
  %s <configFile> [<candidate>...] [--ps1classifier=<ps1classifier>] [--ps2classifier=<ps2classifier>] [--ps1legacyclassifier=<ps1legacyclassifier>] [--ps2legacyclassifier=<ps2legacyclassifier>] [--outputcsv=<outputcsv>] [--comparecsv=<comparecsv>] [--listid=<listid>] [--imageroot=<imageroot>] [--update] [--cnn_data_root=<cnn_data_root>] [--zscale_stats_ps1=<zscale_stats_ps1>] [--zscale_stats_ps2=<zscale_stats_ps2>] [--batch_size=<batch_size>] [--candidatesinfiles] [--trainer=<trainer>]
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
  --outputcsv=<outputcsv>                 PyTorch scores CSV [default: /tmp/pytorch_rb_scores.csv].
  --comparecsv=<comparecsv>               Comparison CSV when legacy classifiers are supplied.
  --imageroot=<imageroot>                 Root location of stamp images [default: /db4/images/].
  --update                                Update the database with PyTorch scores.
  --cnn_data_root=<cnn_data_root>         CNN data root for zscale_stats.json [default: /storage1/software/CNN_PANSTARRS/data].
  --zscale_stats_ps1=<zscale_stats_ps1>   Override PS1 z-scale stats JSON.
  --zscale_stats_ps2=<zscale_stats_ps2>   Override PS2 z-scale stats JSON.
  --batch_size=<batch_size>               Inference batch size [default: 32].
  --candidatesinfiles                     Interpret inline candidate IDs as files.
  --trainer=<trainer>                     Legacy Keras trainer module [default: PSAT-D].

Example:
  cd /storage1/software/CNN_PANSTARRS
  python runPytorchTripletClassifierOnPSATImages.py ~/config.yaml --ps1classifier=cnn_models/cnn_model_ps1.pt --listid=4 --outputcsv=/tmp/ps1_pytorch_list_4.csv
"""
import csv
import os
import sys
from collections import OrderedDict, defaultdict

__doc__ = __doc__ % (sys.argv[0], sys.argv[0], sys.argv[0])

_psat_ml = os.environ.get("PSAT_ML_PATH")
if _psat_ml and _psat_ml not in sys.path:
    sys.path.insert(0, _psat_ml)

from docopt import docopt
from gkutils.commonutils import Struct, cleanOptions, dbConnect

from cnn_inference import get_rb_values_pytorch, median_final_scores
from pytorch_utils import (
    getImages,
    getObjectsByList,
    updateTransientRBValue,
)


def _aggregate_camera_scores(ps1_scores, ps2_scores):
    object_scores = defaultdict(dict)
    if ps1_scores:
        for key, values in ps1_scores.items():
            object_scores[key]["ps1"] = values
    if ps2_scores:
        for key, values in ps2_scores.items():
            object_scores[key]["ps2"] = values
    if not object_scores:
        return {}
    return median_final_scores(object_scores)


def _split_ps_filenames(image_filenames):
    ps1_filenames = []
    ps2_filenames = []
    for row in image_filenames:
        if "00000" in row["filter"]:
            ps1_filenames.append(row["filename"])
        if "00002" in row["filter"]:
            ps2_filenames.append(row["filename"])
    return ps1_filenames, ps2_filenames


def runPytorchTripletClassifier(opts, processNumber=None):
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

    conn = dbConnect(hostname, username, password, database)
    if not conn:
        print("Cannot connect to the database")
        return 1

    conn.autocommit(True)

    ps1_data = bool(options.ps1classifier or options.ps2classifier or options.ps1legacyclassifier or options.ps2legacyclassifier)
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
#    elif processNumber is None:
    else:
        object_list = getObjectsByList(conn, database, listId=int(options.listid), ps1Data=True, imageRoot = options.imageroot)


    image_filenames = []
    if len(object_list) > 0:
        image_filenames = getImages(conn, database, object_list, imageRoot=options.imageroot, ps1Data=True)
        if len(image_filenames) == 0:
            print("NO IMAGES")
            conn.close()
            return []

    ps1_filenames, ps2_filenames = _split_ps_filenames(image_filenames)
    batch_size = int(options.batch_size or 32)
    data_root = options.cnn_data_root

    pytorch_ps1 = {}
    pytorch_ps2 = {}
    legacy_ps1 = {}
    legacy_ps2 = {}

    if ps1_filenames and options.ps1classifier:
        pytorch_ps1 = get_rb_values_pytorch(
            ps1_filenames,
            options.ps1classifier,
            "PS1",
            batch_size=batch_size,
            zscale_stats_path=options.zscale_stats_ps1,
            data_root=data_root,
        )
    if ps2_filenames and options.ps2classifier:
        pytorch_ps2 = get_rb_values_pytorch(
            ps2_filenames,
            options.ps2classifier,
            "PS2",
            batch_size=batch_size,
            zscale_stats_path=options.zscale_stats_ps2,
            data_root=data_root,
        )

    if ps1_filenames and options.ps1legacyclassifier:
        legacy_ps1 = getRBValues(ps1_filenames, options.ps1legacyclassifier, extension=1, trainer=options.trainer)
    if ps2_filenames and options.ps2legacyclassifier:
        legacy_ps2 = getRBValues(ps2_filenames, options.ps2legacyclassifier, extension=1, trainer=options.trainer)

    pytorch_final = _aggregate_camera_scores(pytorch_ps1, pytorch_ps2)
    legacy_final = _aggregate_camera_scores(legacy_ps1, legacy_ps2)

    pytorch_sorted = OrderedDict(sorted(pytorch_final.items(), key=lambda item: item[1]))

    if options.outputcsv is not None and pytorch_sorted:
        prefix = options.outputcsv.split(".")[0]
        suffix = options.outputcsv.split(".")[-1]
        if suffix == prefix:
            suffix = ""
        if suffix:
            suffix = "." + suffix
        process_suffix = ""
        if processNumber is not None:
            process_suffix = "_%d_%03d" % (os.getpid(), processNumber)
        with open("%s%s%s" % (prefix, process_suffix, suffix), "w") as handle:
            for object_id, score in pytorch_sorted.items():
                print(object_id, score)
                handle.write("%s,%f\n" % (object_id, score))

    if options.comparecsv and (legacy_final or pytorch_final):
        compare_path = options.comparecsv
        if processNumber is not None:
            prefix = compare_path.rsplit(".", 1)[0]
            suffix = compare_path.rsplit(".", 1)[-1] if "." in compare_path else "csv"
            compare_path = "%s_%d_%03d.%s" % (prefix, os.getpid(), processNumber, suffix)
        all_ids = sorted(set(pytorch_final) | set(legacy_final), key=lambda x: int(x))
        with open(compare_path, "w", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(["object_id", "pytorch_score", "keras_score"])
            for object_id in all_ids:
                writer.writerow(
                    [
                        object_id,
                        pytorch_final.get(object_id, ""),
                        legacy_final.get(object_id, ""),
                    ]
                )
        print("Wrote comparison CSV:", compare_path)

    scores = list(pytorch_sorted.items())

    if options.update and processNumber is None:
        for object_id, score in scores:
            updateTransientRBValue(conn, object_id, score, ps1Data=True)

    conn.commit()
    conn.close()
    return scores


def main():
    opts = docopt(__doc__, version="0.1")
    opts = cleanOptions(opts)
    options = Struct(**opts)
    runPytorchTripletClassifier(options)


if __name__ == "__main__":
    main()
