#!/usr/bin/env python3
"""
Multiprocess wrapper for Pan-STARRS catalogue XGBoost shadow scoring.
"""

from __future__ import annotations

import argparse
import datetime
import multiprocessing
import os
import sys

import yaml

from xgb_catalogue_utils import (
    db_connect,
    do_xgb_catalogue_classification,
    load_xgb_config_from_yaml,
    summarize_shadow_disagreements,
    write_shadow_log,
)

from machineClassifyTransientsXGB import (
    effective_date_threshold,
    get_object_details,
    get_ps1_objects_shadow,
    parse_date_threshold,
    resolve_shadow_thresholds,
)

LOG_FILE_LOCATION = "/" + os.uname()[1].split(".")[0] + "/tc_logs/"
LOG_PREFIX = "xgb_machineclassification_"


def split_list(object_list: list, bins: int | None = None) -> tuple[int, list[list]]:
    list_length = len(object_list)
    if bins:
        n_processors = bins
    else:
        n_processors = multiprocessing.cpu_count()

    if list_length <= n_processors:
        n_processors = list_length

    chunks: list[list] = [[] for _ in range(n_processors)]
    index = 0
    for item in object_list:
        chunks[index].append(item)
        index += 1
        if index >= n_processors:
            index = 0
    return n_processors, chunks


def worker(
    worker_id: int,
    db_credentials: list[str],
    object_fragment: list[dict],
    date_and_time: str,
    misc_parameters: list,
    queue: multiprocessing.Queue,
) -> int:
    sys.stdout = open(
        f"{LOG_FILE_LOCATION}{LOG_PREFIX}{date_and_time}_{worker_id}.log",
        "w",
        encoding="utf-8",
    )
    xgb_config = misc_parameters[0]
    conn = db_connect(db_credentials[3], db_credentials[0], db_credentials[1], db_credentials[2])
    results = do_xgb_catalogue_classification(conn, object_fragment, xgb_config)
    conn.close()
    print(f"Adding {len(results)} results onto the queue.")
    queue.put(results)
    print("Process complete.")
    return 0


def parallel_process(
    db_credentials: list[str],
    date_and_time: str,
    n_processors: int,
    list_chunks: list[list],
    misc_parameters: list,
) -> list[dict]:
    queues = [multiprocessing.Queue() for _ in range(n_processors)]
    jobs = []
    for index in range(n_processors):
        process = multiprocessing.Process(
            target=worker,
            args=(index, db_credentials, list_chunks[index], date_and_time, misc_parameters, queues[index]),
        )
        jobs.append(process)
        process.start()

    print("Draining objects from the queue.")
    sys.stdout.flush()

    full_results: list[dict] = []
    if n_processors > 1:
        for index in range(n_processors):
            print(f"Draining queue #{index}")
            sys.stdout.flush()
            full_results.extend(queues[index].get())
            print(f"Result count = {len(full_results)}")
    elif n_processors == 1:
        full_results = queues[0].get()
        print(f"Result count = {len(full_results)}")

    print("Waiting for jobs to complete...")
    sys.stdout.flush()
    for job in jobs:
        job.join()
        print("Job complete")
        sys.stdout.flush()

    return full_results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Multiprocess Pan-STARRS catalogue XGBoost classifier (shadow mode)."
    )
    parser.add_argument("config_file", help="Path to config.yaml")
    parser.add_argument("object_ids", nargs="*", type=int, help="Optional object IDs")
    parser.add_argument("--list", type=int, default=4, dest="list_id")
    parser.add_argument("--catalogues", action="store_true")
    parser.add_argument("--shadow", action="store_true", default=False)
    parser.add_argument("--logfile", type=str, default=None)
    parser.add_argument("--date", type=str, default="20130601")
    parser.add_argument("--limit", type=int, default=1_000_000)
    parser.add_argument("--recent-days", type=int, default=14, dest="recent_days")
    parser.add_argument("--rf-threshold", type=float, default=None)
    parser.add_argument("--xgb-threshold", type=float, default=None)
    args = parser.parse_args(argv)

    if not args.catalogues:
        print("ERROR: --catalogues is required.", file=sys.stderr)
        return 1
    if not args.shadow:
        print("ERROR: only --shadow mode is enabled in phase 1.", file=sys.stderr)
        return 1
    if not args.logfile:
        print("ERROR: --shadow requires --logfile", file=sys.stderr)
        return 1

    with open(args.config_file, encoding="utf-8") as yaml_file:
        config = yaml.safe_load(yaml_file)

    db_cfg = config["databases"]["local"]
    db_credentials = [
        db_cfg["username"],
        db_cfg["password"],
        db_cfg["database"],
        db_cfg["hostname"],
    ]
    xgb_config = load_xgb_config_from_yaml(config)
    date_threshold = effective_date_threshold(parse_date_threshold(args.date), args.recent_days)

    conn = db_connect(db_cfg["hostname"], db_cfg["username"], db_cfg["password"], db_cfg["database"])
    if conn is None:
        print("Cannot connect to the database", file=sys.stderr)
        return 1

    if args.object_ids:
        candidate_list = []
        for object_id in args.object_ids:
            details = get_object_details(conn, object_id)
            if details is not None:
                candidate_list.append(details)
    else:
        if args.list_id < 1 or args.list_id > 8:
            print("Detection list must be between 1 and 8", file=sys.stderr)
            conn.close()
            return 1
        candidate_list = get_ps1_objects_shadow(
            conn,
            list_id=args.list_id,
            date_threshold=date_threshold,
            limit=args.limit,
        )
    conn.close()

    print(f"TOTAL OBJECTS TO SCORE = {len(candidate_list)}")
    if not candidate_list:
        try:
            thresholds = resolve_shadow_thresholds(config, args.rf_threshold, args.xgb_threshold)
        except ValueError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 1
        write_shadow_log([], args.logfile, thresholds=thresholds)
        return 0

    current_date = datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")
    year, month, day, hour, minute, second = current_date.split(":")
    date_and_time = f"{year}{month}{day}_{hour}{minute}{second}"

    n_processors, list_chunks = split_list(candidate_list, bins=64)
    print(f"{datetime.datetime.now().strftime('%Y:%m:%d:%H:%M:%S')} Parallel Processing...")
    results = parallel_process(db_credentials, date_and_time, n_processors, list_chunks, [xgb_config])
    print(f"{datetime.datetime.now().strftime('%Y:%m:%d:%H:%M:%S')} Done Parallel Processing")

    try:
        thresholds = resolve_shadow_thresholds(config, args.rf_threshold, args.xgb_threshold)
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    written = write_shadow_log(results, args.logfile, thresholds=thresholds)
    print(
        f"Wrote {written} shadow rows to {args.logfile} "
        f"(RF pass if >= {thresholds.rf_threshold}, "
        f"XGB pass if >= {thresholds.xgb_threshold})"
    )
    try:
        print("\nDisagreement summary:")
        print(summarize_shadow_disagreements(args.logfile).to_string())
    except ValueError:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
