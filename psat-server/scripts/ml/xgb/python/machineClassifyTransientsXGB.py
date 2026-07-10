#!/usr/bin/env python3
"""
Pan-STARRS catalogue XGBoost classifier (Python 3).

Phase 1 (shadow): score live objects and log RF vs XGB scores. Does not update the
database or move objects to garbage.

Usage:
  machineClassifyTransientsXGB.py <config.yaml> [object_id ...]
      [--list 4] [--catalogues] [--shadow] [--logfile PATH]
      [--date YYYYMMDD] [--limit N]
"""

from __future__ import annotations

import argparse
import datetime
import sys
from pathlib import Path

import yaml

from xgb_catalogue_utils import (
    ShadowThresholds,
    XGBCatalogueConfig,
    db_connect,
    do_xgb_catalogue_classification,
    load_shadow_thresholds_from_yaml,
    load_xgb_config_from_yaml,
    summarize_shadow_disagreements,
    write_shadow_log,
)


def get_ps1_objects_shadow(
    conn,
    list_id: int = 4,
    date_threshold: str = "2013-06-01",
    limit: int = 1_000_000,
) -> list[dict]:
    """
    Objects already scored by the RF catalogue classifier (for shadow comparison).
    """
    with conn.cursor() as cursor:
        cursor.execute(
            """
            select o.id,
                   followup_id,
                   ra_psf as ra,
                   dec_psf as `dec`,
                   local_designation as name,
                   ps1_designation,
                   object_classification,
                   local_comments,
                   followup_flag_date,
                   classification_confidence
              from tcs_transient_objects o
             where o.detection_list_id = %s
               and o.classification_confidence is not null
               and o.followup_flag_date >= %s
          order by o.id
             limit %s
            """,
            (list_id, date_threshold, limit),
        )
        return list(cursor.fetchall())


def get_object_details(conn, object_id: int) -> dict | None:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            select o.id,
                   followup_id,
                   ra_psf as ra,
                   dec_psf as `dec`,
                   local_designation as name,
                   object_classification,
                   local_comments,
                   mjd_obs,
                   followup_flag_date,
                   classification_confidence
              from tcs_transient_objects o,
                   tcs_cmf_metadata m
             where o.tcs_cmf_metadata_id = m.id
               and o.id = %s
            """,
            (object_id,),
        )
        return cursor.fetchone()


def parse_date_threshold(date_arg: str | None) -> str:
    if not date_arg:
        return "2013-06-01"
    try:
        return f"{date_arg[0:4]}-{date_arg[4:6]}-{date_arg[6:8]}"
    except (IndexError, ValueError):
        return "2013-06-01"


def effective_date_threshold(date_threshold: str, recent_days: int | None) -> str:
    """Use the more recent of --date and --recent-days (shadow runs stay bounded)."""
    if recent_days is None or recent_days <= 0:
        return date_threshold
    recent = (datetime.date.today() - datetime.timedelta(days=recent_days)).isoformat()
    return max(date_threshold, recent)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Pan-STARRS catalogue XGBoost classifier (shadow mode supported)."
    )
    parser.add_argument("config_file", help="Path to config.yaml")
    parser.add_argument("object_ids", nargs="*", type=int, help="Optional object IDs")
    parser.add_argument("--list", type=int, default=4, dest="list_id", help="Detection list id")
    parser.add_argument(
        "--catalogues",
        action="store_true",
        help="Catalogue-based classification (required for cron compatibility)",
    )
    parser.add_argument(
        "--shadow",
        action="store_true",
        default=False,
        help="Shadow mode: log scores only, no DB updates or garbage moves",
    )
    parser.add_argument("--logfile", type=str, default=None, help="Shadow comparison CSV path")
    parser.add_argument(
        "--date",
        type=str,
        default="20130601",
        help="Followup flag date threshold (YYYYMMDD, no hyphens)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=1_000_000,
        help="Maximum number of objects to score per run",
    )
    parser.add_argument(
        "--recent-days",
        type=int,
        default=14,
        dest="recent_days",
        help="Only score objects flagged within this many days (shadow default: 14)",
    )
    parser.add_argument(
        "--rf-threshold",
        type=float,
        default=None,
        help="Override RF pass threshold (default: catalogue_threshold from config, currently 0.153)",
    )
    parser.add_argument(
        "--xgb-threshold",
        type=float,
        default=None,
        help="Override XGB shadow pass threshold (default: shadow_threshold from config)",
    )
    parser.add_argument(
        "--summarize-log",
        type=str,
        default=None,
        help="Print pass/fail disagreement counts from an existing shadow CSV and exit",
    )
    return parser


def resolve_shadow_thresholds(config: dict, rf_override: float | None, xgb_override: float | None) -> ShadowThresholds:
    thresholds = load_shadow_thresholds_from_yaml(config)
    if rf_override is not None:
        thresholds = ShadowThresholds(rf_threshold=rf_override, xgb_threshold=thresholds.xgb_threshold)
    if xgb_override is not None:
        thresholds = ShadowThresholds(rf_threshold=thresholds.rf_threshold, xgb_threshold=xgb_override)
    return thresholds


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    if args.summarize_log:
        summary = summarize_shadow_disagreements(args.summarize_log)
        print(summary.to_string())
        return 0

    if not args.catalogues:
        print("ERROR: --catalogues is required for catalogue XGB scoring.", file=sys.stderr)
        return 1

    if not args.shadow:
        print(
            "ERROR: only --shadow mode is enabled in phase 1. "
            "Refusing to run without --shadow.",
            file=sys.stderr,
        )
        return 1

    with open(args.config_file, encoding="utf-8") as yaml_file:
        config = yaml.safe_load(yaml_file)

    db_cfg = config["databases"]["local"]
    conn = db_connect(
        db_cfg["hostname"],
        db_cfg["username"],
        db_cfg["password"],
        db_cfg["database"],
    )
    if conn is None:
        print("Cannot connect to the database", file=sys.stderr)
        return 1

    xgb_config: XGBCatalogueConfig = load_xgb_config_from_yaml(config)
    date_threshold = effective_date_threshold(parse_date_threshold(args.date), args.recent_days)

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

    print(f"TOTAL OBJECTS TO SCORE = {len(candidate_list)}")
    results = do_xgb_catalogue_classification(conn, candidate_list, xgb_config)
    conn.close()

    if args.shadow:
        if not args.logfile:
            print("ERROR: --shadow requires --logfile", file=sys.stderr)
            return 1
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
