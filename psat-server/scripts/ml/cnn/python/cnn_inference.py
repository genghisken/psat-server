"""
PyTorch triplet-CNN inference for Pan-STARRS postage stamps.

Loads target/ref/diff FITS triplets with z-scale normalisation and returns
per-image real-bogus probabilities compatible with the legacy psat-ml pipeline.
"""

from __future__ import annotations

import logging
import os
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import torch

from cnn_data import (
    CNN_DATA_ROOT,
    IMAGE_SIZE,
    load_zscale_stats,
    preprocess_numpy,
    read_triplet,
    resolve_diff_fits_path,
    zscale_stats_path_for_camera,
)
from panstarrs_cnn_model import PanSTARRSCNN

LOGGER = logging.getLogger(__name__)


@dataclass
class PyTorchClassifierBundle:
    model: PanSTARRSCNN
    device: torch.device
    decision_threshold: float
    lowers: np.ndarray
    uppers: np.ndarray
    camera: str
    model_path: Path


def pick_device() -> torch.device:
    if torch.cuda.is_available():
        return torch.device("cuda")
    return torch.device("cpu")


def resolve_zscale_stats_path(
    camera: str,
    *,
    explicit: str | Path | None = None,
    model_path: str | Path | None = None,
    data_root: str | Path | None = None,
) -> Path:
    """Locate camera-specific zscale_stats.json for inference."""
    if explicit:
        path = Path(explicit)
        if path.is_file():
            return path
        raise FileNotFoundError(f"zscale stats not found: {path}")

    camera = camera.upper()
    roots: list[Path] = []
    if data_root:
        roots.append(Path(data_root))
    if model_path:
        model_path = Path(model_path)
        roots.append(model_path.parent.parent / "data")
        roots.append(model_path.parent)
    roots.append(CNN_DATA_ROOT)

    for root in roots:
        candidate = zscale_stats_path_for_camera(root, camera)
        if candidate.is_file():
            return candidate

    raise FileNotFoundError(
        f"Could not find zscale_stats.json for {camera}. "
        "Pass --zscale-stats-ps1/--zscale-stats-ps2 or --cnn-data-root."
    )


def load_pytorch_classifier(
    model_path: str | Path,
    camera: str,
    *,
    zscale_stats_path: str | Path | None = None,
    data_root: str | Path | None = None,
    device: torch.device | None = None,
) -> PyTorchClassifierBundle:
    """Load a trained .pt checkpoint and matching z-scale stats."""
    model_path = Path(model_path)
    if not model_path.is_file():
        raise FileNotFoundError(model_path)

    camera = camera.upper()
    device = device or pick_device()
    stats_path = resolve_zscale_stats_path(
        camera,
        explicit=zscale_stats_path,
        model_path=model_path,
        data_root=data_root,
    )
    lowers, uppers = load_zscale_stats(stats_path)

    checkpoint = torch.load(model_path, map_location=device)
    model = PanSTARRSCNN().to(device)
    model.load_state_dict(checkpoint["model_state_dict"])
    model.eval()

    threshold = float(checkpoint.get("decision_threshold", 0.5))
    LOGGER.info(
        "Loaded PyTorch CNN %s for %s (threshold=%.3f, z-scale from %s)",
        model_path,
        camera,
        threshold,
        stats_path,
    )
    return PyTorchClassifierBundle(
        model=model,
        device=device,
        decision_threshold=threshold,
        lowers=lowers,
        uppers=uppers,
        camera=camera,
        model_path=model_path,
    )


def _candidate_id_from_filename(image_filename: str) -> str:
    return os.path.basename(image_filename).split("_")[0]


def predict_triplet_probabilities(
    image_filenames: list[str],
    bundle: PyTorchClassifierBundle,
    *,
    batch_size: int = 32,
    crop_size: int = IMAGE_SIZE,
    replace_from: str | None = None,
    replace_to: str | None = None,
) -> tuple[list[str], np.ndarray]:
    """
    Run inference on diff (or DB-style) FITS paths.

    Returns (filenames_used, probabilities) aligned arrays. Missing triplets
    are skipped.
    """
    valid_paths: list[str] = []
    tensors: list[np.ndarray] = []

    for path in image_filenames:
        resolved = resolve_diff_fits_path(path)
        if resolved is None:
            LOGGER.warning("Skipping missing diff stamp: %s", path)
            continue
        chw = read_triplet(
            resolved,
            crop_size=crop_size,
            replace_from=replace_from,
            replace_to=replace_to,
            quiet=True,
        )
        if chw is None:
            LOGGER.warning("Skipping unreadable triplet for %s", path)
            continue
        hwc = preprocess_numpy(chw, bundle.lowers, bundle.uppers)
        tensors.append(hwc.transpose(2, 0, 1))
        valid_paths.append(path)

    if not tensors:
        return [], np.array([], dtype=np.float32)

    probs: list[float] = []
    model = bundle.model
    device = bundle.device

    with torch.no_grad():
        for start in range(0, len(tensors), batch_size):
            batch = np.stack(tensors[start : start + batch_size], axis=0)
            tensor = torch.from_numpy(batch).float().to(device)
            pred = torch.sigmoid(model(tensor)).cpu().numpy().flatten()
            probs.extend(float(x) for x in pred)

    return valid_paths, np.asarray(probs, dtype=np.float32)


def scores_by_object(
    image_filenames: list[str],
    probabilities: np.ndarray,
) -> dict[str, list[float]]:
    """Group per-stamp probabilities by transient object id (legacy aggregation)."""
    object_dict: dict[str, list[float]] = defaultdict(list)
    for path, prob in zip(image_filenames, probabilities):
        object_dict[_candidate_id_from_filename(path)].append(float(prob))
    return dict(object_dict)


def get_rb_values_pytorch(
    image_filenames: list[str],
    classifier_path: str | Path,
    camera: str,
    *,
    batch_size: int = 32,
    zscale_stats_path: str | Path | None = None,
    data_root: str | Path | None = None,
    replace_from: str | None = None,
    replace_to: str | None = None,
) -> dict[str, list[float]]:
    """
    Drop-in analogue of runKerasTensorflowClassifierOnPSATImages.getRBValues.

    Returns {object_id: [prob_real, ...]} for each stamp belonging to that object.
    """
    bundle = load_pytorch_classifier(
        classifier_path,
        camera,
        zscale_stats_path=zscale_stats_path,
        data_root=data_root,
    )
    used_paths, probs = predict_triplet_probabilities(
        image_filenames,
        bundle,
        batch_size=batch_size,
        replace_from=replace_from,
        replace_to=replace_to,
    )
    return scores_by_object(used_paths, probs)


def median_final_scores(object_scores: dict[str, dict[str, list[float]]]) -> dict[str, float]:
    """
    Combine PS1/PS2 (or multi-telescope) score lists like the legacy runner:
    pick the camera key with the most stamps, then median.
    """
    final_scores: dict[str, float] = {}
    for object_id, camera_scores in object_scores.items():
        lengths = {key: len(values) for key, values in camera_scores.items()}
        best_key = max(lengths, key=lambda key: lengths[key])
        final_scores[object_id] = float(np.median(camera_scores[best_key]))
    return final_scores
