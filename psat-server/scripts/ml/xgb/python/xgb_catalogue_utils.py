"""
Pan-STARRS catalogue XGBoost utilities (Python 3).

Shadow-mode scoring for live pipeline objects. Model training lives in 5.XGBOOST.py.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pymysql
import xgboost as xgb

# Must match 5.XGBOOST.py / training_meta_ps*.json feature order.
XGB_FEATURES = [
    "x_psf",
    "y_psf",
    "x_psf_sig",
    "y_psf_sig",
    "psf_inst_mag",
    "psf_inst_mag_sig",
    "ap_mag",
    "cal_psf_mag",
    "sky",
    "sky_sigma",
    "psf_chisq",
    "ext_nsigma",
    "psf_major",
    "psf_minor",
    "psf_theta",
    "psf_qf",
    "psf_ndof",
    "psf_npix",
    "moments_xx",
    "moments_xy",
    "moments_yy",
    "psf_inst_flux",
    "psf_inst_flux_sig",
    "diff_npos",
    "diff_fratio",
    "diff_nratio_bad",
    "diff_nratio_mask",
    "diff_nratio_all",
    "ap_flux",
    "ap_flux_sig",
    "ap_mag_raw",
    "diff_r_m",
    "diff_r_p",
    "diff_sn_m",
    "diff_sn_p",
    "flags",
    "flags2",
    "kron_flux",
    "kron_flux_err",
    "kron_flux_inner",
    "moments_r1",
    "moments_rh",
    "psf_qf_perfect",
]


@dataclass
class XGBCatalogueConfig:
    catalogue_path: str
    catalogue_classifier_ps1: str
    catalogue_classifier_ps2: str
    features: list[str]
    time_window: float = 2.0
    n_detections: int = 4
    shadow_threshold: float | None = None


@dataclass
class ShadowThresholds:
    """Pass/fail cutoffs for shadow comparison (score >= threshold means keep on list)."""

    rf_threshold: float
    xgb_threshold: float


def get_mjd_from_sql_date(sql_date: str) -> float | None:
    try:
        year, month, day = sql_date[0:10].split("-")
        hours, minutes, seconds = sql_date[11:19].split(":")
        t = (int(year), int(month), int(day), int(hours), int(minutes), int(seconds), 0, 0, 0)
        unixtime = int(time.mktime(t))
        return unixtime / 86400.0 - 2400000.5 + 2440587.5
    except (ValueError, IndexError):
        return None


def db_connect(hostname: str, username: str, password: str, database: str):
    return pymysql.connect(
        host=hostname,
        user=username,
        password=password,
        database=database,
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )


def load_xgb_config_from_yaml(config: dict) -> XGBCatalogueConfig:
    xgb_cfg = config["machine_learning"]["xgb_catalogue_classifier_components"]
    shadow_threshold = xgb_cfg.get("shadow_threshold")
    return XGBCatalogueConfig(
        catalogue_path=xgb_cfg["catalogue_path"],
        catalogue_classifier_ps1=xgb_cfg["catalogue_classifier_ps1"],
        catalogue_classifier_ps2=xgb_cfg["catalogue_classifier_ps2"],
        features=list(xgb_cfg.get("features", XGB_FEATURES)),
        time_window=float(xgb_cfg.get("time_window", 2)),
        n_detections=int(xgb_cfg.get("n_detections", 4)),
        shadow_threshold=float(shadow_threshold) if shadow_threshold is not None else None,
    )


def load_shadow_thresholds_from_yaml(config: dict) -> ShadowThresholds:
    ml_cfg = config["machine_learning"]
    rf_threshold = float(ml_cfg["catalogue_classifier_components"]["catalogue_threshold"])
    xgb_threshold = ml_cfg["xgb_catalogue_classifier_components"].get("shadow_threshold")
    if xgb_threshold is None:
        raise ValueError(
            "machine_learning.xgb_catalogue_classifier_components.shadow_threshold is required "
            "for shadow pass/fail comparison."
        )
    return ShadowThresholds(rf_threshold=rf_threshold, xgb_threshold=float(xgb_threshold))


def score_would_pass(score: float | None, threshold: float) -> bool | None:
    """True = would stay on the eyeball list; False = would move to garbage."""
    if score is None:
        return None
    return float(score) >= float(threshold)


def compare_pass_decisions(rf_pass: bool | None, xgb_pass: bool | None) -> str:
    if rf_pass is None or xgb_pass is None:
        return "incomplete"
    if rf_pass and xgb_pass:
        return "both_pass"
    if not rf_pass and not xgb_pass:
        return "both_fail"
    if rf_pass and not xgb_pass:
        return "rf_pass_xgb_fail"
    return "rf_fail_xgb_pass"


def load_xgb_models(xgb_config: XGBCatalogueConfig) -> tuple[xgb.XGBClassifier, xgb.XGBClassifier]:
    base = Path(xgb_config.catalogue_path)
    ps1_path = base / xgb_config.catalogue_classifier_ps1
    ps2_path = base / xgb_config.catalogue_classifier_ps2
    for path in (ps1_path, ps2_path):
        if not path.is_file():
            raise FileNotFoundError(f"XGB model not found: {path}")

    ps1_model = xgb.XGBClassifier()
    ps1_model.load_model(str(ps1_path))
    ps2_model = xgb.XGBClassifier()
    ps2_model.load_model(str(ps2_path))
    return ps1_model, ps2_model




def prepare_feature_frame(rows: list[dict], features: list[str]) -> pd.DataFrame:
    data = {feature: [] for feature in features}

    for row in rows:
        for feature in features:
            value = row.get(feature)

            if value is None:
                data[feature].append(np.nan)
                continue

            if feature in {"flags", "flags2"}:
                # Store as strings first so category labels are not floats.
                try:
                    data[feature].append(str(int(value)))
                except (TypeError, ValueError):
                    data[feature].append(None)
                continue

            try:
                data[feature].append(float(value))
            except (TypeError, ValueError):
                data[feature].append(np.nan)

    frame = pd.DataFrame(data)

    for col in ("flags", "flags2"):
        if col in frame.columns:
            frame[col] = frame[col].astype("string").astype("category")

    return frame

# ----> def prepare_feature_frame(rows: list[dict], features: list[str]) -> pd.DataFrame:
# ----->     data = {feature: [] for feature in features}
# ----->     for row in rows:
# ----->         for feature in features:
# ----->             value = row.get(feature)
# ----->             if value is None:
# ----->                 data[feature].append(np.nan)
# ----->             else:
# ----->                 try:
# ----->                     data[feature].append(float(value))
# ----->                 except (TypeError, ValueError):
# ----->                     data[feature].append(np.nan)
# -----> 
# ----->     frame = pd.DataFrame(data)
# -----> #    if "flags" in frame.columns:
# -----> #        frame["flags"] = frame["flags"].astype("category")
# -----> #    if "flags2" in frame.columns:
# -----> #        frame["flags2"] = frame["flags2"].astype("category")
# -----> 
# ----->     for col in ("flags", "flags2"):
# ----->         if col in frame.columns:
# ----->             frame[col] = frame[col].map(lambda v: None if pd.isna(v) else str(int(v))).astype("category")
# ----->     return frame


def median_predict_proba(model: xgb.XGBClassifier, rows: list[dict], features: list[str]) -> float | None:
    if not rows:
        return None
    frame = prepare_feature_frame(rows, features)
    probabilities = model.predict_proba(frame)[:, 1]
    return float(np.median(probabilities))


def split_detections_by_camera(
    rows: list[dict],
    candidate: dict,
    time_window: float,
    n_detections: int,
) -> tuple[list[dict], list[dict]]:
    ps1_rows: list[dict] = []
    ps2_rows: list[dict] = []
    followup_date = candidate.get("followup_flag_date")

    if followup_date:
        followup_mjd = get_mjd_from_sql_date(followup_date.strftime("%Y-%m-%d") + " 00:00:00")
        if followup_mjd is not None:
            followup_mjd += 1.0
            for row in rows:
                mjd = row.get("mjd_obs")
                if mjd is None:
                    continue
                if mjd <= followup_mjd and mjd > followup_mjd - time_window:
                    if row.get("fpa_detector") == "GPC1":
                        ps1_rows.append(row)
                    elif row.get("fpa_detector") == "GPC2":
                        ps2_rows.append(row)
            if not ps1_rows and not ps2_rows:
                for row in rows:
                    mjd = row.get("mjd_obs")
                    if mjd is None:
                        continue
                    if mjd <= followup_mjd and mjd > followup_mjd - time_window * 2:
                        if row.get("fpa_detector") == "GPC1":
                            ps1_rows.append(row)
                        elif row.get("fpa_detector") == "GPC2":
                            ps2_rows.append(row)
            if not ps1_rows and not ps2_rows:
                for row in rows[-n_detections:]:
                    if row.get("fpa_detector") == "GPC1":
                        ps1_rows.append(row)
                    elif row.get("fpa_detector") == "GPC2":
                        ps2_rows.append(row)
    else:
        for row in rows[-n_detections:]:
            if row.get("fpa_detector") == "GPC1":
                ps1_rows.append(row)
            elif row.get("fpa_detector") == "GPC2":
                ps2_rows.append(row)

    return ps1_rows, ps2_rows


def combine_camera_scores(
    ps1_score: float | None,
    ps2_score: float | None,
    n_ps1: int,
    n_ps2: int,
) -> float | None:
    if n_ps1 > n_ps2:
        return ps1_score
    if n_ps2 > n_ps1:
        return ps2_score
    if n_ps1 == n_ps2 and n_ps1 > 0:
        scores = [score for score in (ps1_score, ps2_score) if score is not None]
        return max(scores) if scores else None
    return None


def fetch_detection_rows(conn, object_id: int, features: list[str]) -> list[dict]:
    features_sql = ", ".join(features)
    query = f"""
        select o.id, {features_sql}, mjd_obs, fpa_detector
          from tcs_transient_objects o,
               tcs_cmf_metadata m
         where o.id = %s
           and o.tcs_cmf_metadata_id = m.id
     union all
        select transient_object_id as id, {features_sql}, mjd_obs, fpa_detector
          from tcs_transient_reobservations r,
               tcs_cmf_metadata m
         where r.transient_object_id = %s
           and r.tcs_cmf_metadata_id = m.id
      order by mjd_obs
    """
    with conn.cursor() as cursor:
        cursor.execute(query, (object_id, object_id))
        return list(cursor.fetchall())


def score_candidate(
    conn,
    candidate: dict,
    ps1_model: xgb.XGBClassifier,
    ps2_model: xgb.XGBClassifier,
    xgb_config: XGBCatalogueConfig,
) -> dict[str, Any]:
    object_id = int(candidate["id"])
    features = xgb_config.features
    rows = fetch_detection_rows(conn, object_id, features)
    ps1_rows, ps2_rows = split_detections_by_camera(
        rows,
        candidate,
        xgb_config.time_window,
        xgb_config.n_detections,
    )
    ps1_score = median_predict_proba(ps1_model, ps1_rows, features)
    ps2_score = median_predict_proba(ps2_model, ps2_rows, features)
    xgb_score = combine_camera_scores(ps1_score, ps2_score, len(ps1_rows), len(ps2_rows))

    return {
        "candidate": candidate,
        "xgb_score": xgb_score,
        "ps1_xgb_score": ps1_score,
        "ps2_xgb_score": ps2_score,
        "n_ps1_detections": len(ps1_rows),
        "n_ps2_detections": len(ps2_rows),
    }


def do_xgb_catalogue_classification(
    conn,
    candidate_list: list[dict],
    xgb_config: XGBCatalogueConfig,
    ps1_model: xgb.XGBClassifier | None = None,
    ps2_model: xgb.XGBClassifier | None = None,
) -> list[dict[str, Any]]:
    if ps1_model is None or ps2_model is None:
        ps1_model, ps2_model = load_xgb_models(xgb_config)

    results: list[dict[str, Any]] = []
    for candidate in candidate_list:
        try:
            result = score_candidate(conn, candidate, ps1_model, ps2_model, xgb_config)
        except Exception as exc:
            result = {
                "candidate": candidate,
                "xgb_score": None,
                "ps1_xgb_score": None,
                "ps2_xgb_score": None,
                "n_ps1_detections": 0,
                "n_ps2_detections": 0,
                "error": str(exc),
            }
        results.append(result)
        print(f"{candidate['id']} {result.get('xgb_score')}")

    return results


def write_shadow_log(
    results: list[dict[str, Any]],
    logfile: str | Path,
    thresholds: ShadowThresholds | None = None,
) -> int:
    log_path = Path(logfile)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not log_path.exists() or log_path.stat().st_size == 0

    rows = []
    for result in results:
        candidate = result["candidate"]
        followup = candidate.get("followup_flag_date")
        followup_str = followup.strftime("%Y-%m-%d") if followup is not None else ""
        rf_score = candidate.get("classification_confidence")
        xgb_score = result.get("xgb_score")

        row = {
            "object_id": candidate["id"],
            "followup_flag_date": followup_str,
            "rf_classification_confidence": rf_score,
            "xgb_score": xgb_score,
            "ps1_xgb_score": result.get("ps1_xgb_score"),
            "ps2_xgb_score": result.get("ps2_xgb_score"),
            "n_ps1_detections": result.get("n_ps1_detections"),
            "n_ps2_detections": result.get("n_ps2_detections"),
            "error": result.get("error", ""),
        }

        if thresholds is not None:
            rf_pass = score_would_pass(rf_score, thresholds.rf_threshold)
            xgb_pass = score_would_pass(xgb_score, thresholds.xgb_threshold)
            row.update(
                {
                    "rf_threshold": thresholds.rf_threshold,
                    "xgb_shadow_threshold": thresholds.xgb_threshold,
                    "rf_would_pass": rf_pass,
                    "xgb_would_pass": xgb_pass,
                    "pass_agreement": compare_pass_decisions(rf_pass, xgb_pass),
                }
            )

        rows.append(row)

    frame = pd.DataFrame(rows)
    frame.to_csv(log_path, mode="a", header=write_header, index=False)
    return len(rows)


def summarize_shadow_disagreements(logfile: str | Path) -> pd.DataFrame:
    """Quick lookup: how often RF and XGB agree on pass/fail."""
    frame = pd.read_csv(logfile)
    if "pass_agreement" not in frame.columns:
        raise ValueError("Shadow log has no pass/fail columns — was shadow_threshold set in config?")
    return frame["pass_agreement"].value_counts().sort_index()


def default_log_file_location() -> str:
    host = os.uname()[1].split(".")[0]
    return f"/{host}/tc_logs/xgb_shadow.log"
