"""
Shared CNN data utilities: FITS triplets, HDF5 I/O, preprocessing, PyTorch Dataset.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

import h5py
import numpy as np
import torch
from torch.utils.data import Dataset

LOGGER = logging.getLogger(__name__)

# Default project layout on astrosurveydb1:
#   /storage1/software/CNN_PANSTARRS/
#     PS1_stamps_paths.tst
#     data/PS1/{train,valid,test}.h5, zscale_stats.json
#     cnn_models/cnn_model_ps1.pt, ...
CNN_PROJECT_ROOT = Path("/storage1/software/CNN_PANSTARRS")
CNN_DATA_ROOT = CNN_PROJECT_ROOT / "data"
CNN_MODEL_DIR = CNN_PROJECT_ROOT / "cnn_models"

# 96×96 matches the legacy TFRecord/CNN pipeline and pools cleanly (96→48→24→12→6→3→1).
IMAGE_SIZE = 96
CHANNEL_ORDER = ("target", "ref", "diff")
ZSCALE_STATS_FILENAME = "zscale_stats.json"

# Original multi-class labels stored in HDF5 (PUNK-style).
ORIG_LABEL_NAMES = {0: "Garbage", 1: "Good", 2: "Movers", 3: "NT"}

CLASS_TO_ORIG_LABEL = {
    "Garbage": 0,
    "AutoGarbage": 0,
    "SN": 1,
    "Asteroid": 2,
    "NT": 3,
}

LOWERS = np.array([-48.599609, -332.437500, -55.829075], dtype=np.float32)
UPPERS = np.array([428.073047, 4179.332031, 85.310239], dtype=np.float32)


def zscale_stats_path_for_camera(data_root: Path, camera: str) -> Path:
    return data_root / camera.upper() / ZSCALE_STATS_FILENAME


def zscale_stats_path_for_h5(h5_path: Path) -> Path:
    return Path(h5_path).parent / ZSCALE_STATS_FILENAME


def compute_zscale_stats(
    h5_path: str | Path,
    max_samples: int = 5000,
    max_pixels_per_channel: int = 500_000,
    seed: int = 42,
) -> dict[str, Any]:
    """
    One z-scale interval per channel type (target / ref / diff) from the **train** HDF5.

    Pools pixels from a random subset of training cutouts, then runs astropy ZScaleInterval
    separately on each channel — so all targets share one (vmin, vmax), etc.
    """
    try:
        from astropy.visualization import ZScaleInterval
    except ImportError as exc:
        raise ImportError("Z-scale stats require astropy: pip install astropy") from exc

    h5_path = Path(h5_path)
    rng = np.random.default_rng(seed)
    zscale = ZScaleInterval()

    with h5py.File(h5_path, "r") as handle:
        n_total = int(handle["images"].shape[0])
        image_size = int(handle["images"].shape[-1])
        n_sample = min(n_total, max_samples)
        indices = rng.choice(n_total, size=n_sample, replace=False) if n_total > n_sample else np.arange(n_total)

        channels: dict[str, dict[str, float]] = {}
        for ch, name in enumerate(CHANNEL_ORDER):
            chunks: list[np.ndarray] = []
            for idx in indices:
                plane = handle["images"][int(idx), ch].astype(np.float32)
                chunks.append(plane.ravel())
            pixels = np.concatenate(chunks)
            pixels = pixels[np.isfinite(pixels)]
            if pixels.size == 0:
                raise RuntimeError(f"No finite pixels for channel {name} in {h5_path}")
            if pixels.size > max_pixels_per_channel:
                pixels = rng.choice(pixels, size=max_pixels_per_channel, replace=False)
            vmin, vmax = zscale.get_limits(pixels)
            if vmax <= vmin:
                vmax = vmin + 1.0
            channels[name] = {"vmin": float(vmin), "vmax": float(vmax)}
            LOGGER.info(
                "Z-scale %s from %d train samples: vmin=%.4g vmax=%.4g",
                name,
                n_sample,
                vmin,
                vmax,
            )

    return {
        "method": "zscale_per_channel",
        "source_h5": str(h5_path),
        "n_samples": int(n_sample),
        "image_size": image_size,
        "channels": channels,
    }


def save_zscale_stats(stats_path: str | Path, stats: dict[str, Any]) -> None:
    stats_path = Path(stats_path)
    stats_path.parent.mkdir(parents=True, exist_ok=True)
    stats_path.write_text(json.dumps(stats, indent=2), encoding="utf-8")
    LOGGER.info("Wrote z-scale stats: %s", stats_path)


def load_zscale_stats(stats_path: str | Path) -> tuple[np.ndarray, np.ndarray]:
    """Return (lowers, uppers) float32 arrays of shape (3,) in target/ref/diff order."""
    stats_path = Path(stats_path)
    payload = json.loads(stats_path.read_text(encoding="utf-8"))
    channels = payload["channels"]
    lowers = np.array([channels[name]["vmin"] for name in CHANNEL_ORDER], dtype=np.float32)
    uppers = np.array([channels[name]["vmax"] for name in CHANNEL_ORDER], dtype=np.float32)
    return lowers, uppers

CAMERA_SPLITS = {
    "PS1": {"train": "train.h5", "valid": "valid.h5", "test": "test.h5"},
    "PS2": {"train": "train.h5", "valid": "valid.h5", "test": "test.h5"},
}


def stamps_paths_file(camera: str) -> Path:
    return CNN_PROJECT_ROOT / f"{camera.upper()}_stamps_paths.tst"


def collapse_binary_label(orig_label: int | np.ndarray) -> int | np.ndarray:
    """PUNK mapping: 2 and 3 -> 1; 0 stays 0; 1 stays 1."""
    out = np.where(orig_label == 2, 1, orig_label)
    return np.where(out == 3, 1, out)


def triplet_paths_from_diff(diff_path: str) -> tuple[str, str, str]:
    """Return (target, ref, diff) paths from a *_diff.fits path."""
    if not diff_path.endswith("_diff.fits"):
        raise ValueError(f"Expected *_diff.fits path, got {diff_path!r}")
    base = diff_path[: -len("_diff.fits")]
    return f"{base}_target.fits", f"{base}_ref.fits", diff_path


def resolve_diff_fits_path(path: str) -> str | None:
    """
    Resolve an on-disk diff FITS path from catalog or DB conventions.

    The psat-ml DB query returns ``image_filename + '.fits'`` for diff rows where
    ``image_filename`` is the stamp base (no type suffix). On disk the triplet is
    usually ``{base}_diff.fits``, ``{base}_target.fits``, ``{base}_ref.fits``.
    """
    path = path.strip()
    if not path:
        return None

    candidates: list[str] = []
    if path.endswith("_diff.fits"):
        candidates.append(path)
    elif path.endswith(".fits"):
        base = path[: -len(".fits")]
        candidates.extend([f"{base}_diff.fits", path])
    else:
        candidates.extend([f"{path}_diff.fits", f"{path}.fits"])

    seen: set[str] = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        if os.path.isfile(candidate):
            return candidate
    return None


def triplet_paths_from_stamp(path: str) -> tuple[str, str, str] | None:
    """Return (target, ref, diff) paths for a diff or DB-style stamp path."""
    diff_path = resolve_diff_fits_path(path)
    if diff_path is None:
        return None
    if diff_path.endswith("_diff.fits"):
        base = diff_path[: -len("_diff.fits")]
        return f"{base}_target.fits", f"{base}_ref.fits", diff_path
    base = diff_path[: -len(".fits")]
    return f"{base}_target.fits", f"{base}_ref.fits", diff_path


def remap_path(path: str, replace_from: str | None, replace_to: str | None) -> str:
    if replace_from and replace_to and path.startswith(replace_from):
        return replace_to + path[len(replace_from) :]
    return path


def crop_center(image: np.ndarray, crop_size: int = IMAGE_SIZE) -> np.ndarray:
    h, w = image.shape
    start_x = max((w - crop_size) // 2, 0)
    start_y = max((h - crop_size) // 2, 0)
    return image[start_y : start_y + crop_size, start_x : start_x + crop_size]


def read_fits_plane(file_path: str, quiet: bool = False) -> np.ndarray | None:
    try:
        from astropy.io import fits
    except ImportError as exc:
        raise ImportError("Reading FITS requires astropy: pip install astropy") from exc

    try:
        with fits.open(file_path) as hdul:
            data = hdul[1].data.astype(np.float32)
            if "ZBLANK" in hdul[1].header:
                zblank = hdul[1].header["ZBLANK"]
                data[data == zblank] = np.nan
        return data
    except Exception as exc:
        if not quiet:
            LOGGER.warning("Failed to read %s: %s", file_path, exc)
        return None


def read_triplet(
    diff_path: str,
    crop_size: int = IMAGE_SIZE,
    replace_from: str | None = None,
    replace_to: str | None = None,
    quiet: bool = False,
) -> np.ndarray | None:
    """Load target/ref/diff FITS and return float32 array (3, H, W)."""
    remapped = remap_path(diff_path, replace_from, replace_to)
    triplet = triplet_paths_from_stamp(remapped)
    if triplet is None:
        if not quiet:
            LOGGER.warning("Could not resolve diff stamp path: %s", diff_path)
        return None
    target_path, ref_path, diff_path = triplet
    planes = []
    for path in (target_path, ref_path, diff_path):
        data = read_fits_plane(path, quiet=quiet)
        if data is None:
            return None
        planes.append(crop_center(data, crop_size))
    return np.stack(planes, axis=0).astype(np.float32)


def preprocess_numpy(
    images: np.ndarray,
    lowers: np.ndarray | None = None,
    uppers: np.ndarray | None = None,
) -> np.ndarray:
    """
    Intensity-normalise a single triplet to (H, W, 3) in [0, 1] per channel.

    Spatial size is already 96×96 from FITS crop. Here "clip" means clamp pixel
    *values* to each channel's z-scale vmin/vmax (from zscale_stats.json), then
    linearly rescale to [0, 1] — not a further spatial crop.
    """
    if lowers is None or uppers is None:
        lowers, uppers = LOWERS, UPPERS

    out_planes = []
    for ch in range(3):
        plane = np.nan_to_num(images[ch].astype(np.float32), nan=0.0)
        lo, hi = float(lowers[ch]), float(uppers[ch])
        scaled = (np.clip(plane, lo, hi) - lo) / (hi - lo + 1e-8)
        out_planes.append(scaled)
    return np.stack(out_planes, axis=-1).astype(np.float32)


def preprocess_batch(
    images: np.ndarray,
    lowers: np.ndarray | None = None,
    uppers: np.ndarray | None = None,
) -> np.ndarray:
    """Vectorized preprocess for (N, 3, H, W) -> (N, H, W, 3)."""
    return np.stack([preprocess_numpy(img, lowers, uppers) for img in images], axis=0)


# HDF5 chunk size used at build time — shuffle within chunks for sequential disk reads.
H5_IMAGE_CHUNK = 256
H5_RDCC_NBYTES = 256 * 1024 * 1024


def split_h5_path(data_root: Path, camera: str, split: str) -> Path:
    camera = camera.upper()
    if camera not in CAMERA_SPLITS:
        raise ValueError(f"camera must be PS1 or PS2, got {camera!r}")
    if split not in CAMERA_SPLITS[camera]:
        raise ValueError(f"split must be train, valid, or test, got {split!r}")
    return data_root / camera / CAMERA_SPLITS[camera][split]


def count_binary_labels_in_h5(h5_path: str | Path) -> tuple[int, int, int]:
    """Return (n_negative, n_positive, n_total) after binary collapse."""
    with h5py.File(h5_path, "r") as handle:
        orig = handle["labels"][:].astype(np.int64)
    binary = collapse_binary_label(orig)
    n_pos = int(np.sum(binary == 1))
    n_neg = int(np.sum(binary == 0))
    return n_neg, n_pos, int(len(orig))


def count_orig_labels_in_h5(h5_path: str | Path) -> dict[int, int]:
    with h5py.File(h5_path, "r") as handle:
        orig = handle["labels"][:].astype(np.int64)
    return {int(lab): int(np.sum(orig == lab)) for lab in np.unique(orig)}


def write_h5_shard(
    output_path: Path,
    images: np.ndarray,
    labels: np.ndarray,
    stamps: list[str],
) -> None:
    """Write one worker shard (uncompressed, fixed size — fast)."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    str_dtype = h5py.string_dtype(encoding="utf-8")
    image_size = int(images.shape[-1])
    with h5py.File(output_path, "w") as handle:
        handle.create_dataset(
            "images",
            data=images.astype(np.float32),
            chunks=(min(256, len(labels)), images.shape[1], image_size, image_size),
        )
        handle.create_dataset("labels", data=labels.astype(np.int64))
        handle.create_dataset(
            "detection_stamps",
            data=np.array(stamps, dtype=object),
            dtype=str_dtype,
        )
        handle.attrs["image_size"] = image_size
        handle.attrs["channels"] = images.shape[1]
        handle.attrs["channel_order"] = ",".join(CHANNEL_ORDER)


def merge_h5_shards(
    shard_paths: list[Path | str],
    output_path: Path,
    compression: str | None = None,
    compression_opts: int = 4,
    delete_shards: bool = True,
) -> int:
    """Concatenate shard HDF5 files into one split file (pre-allocated — steady speed)."""
    ordered = sorted(Path(p) for p in shard_paths if p is not None)
    if not ordered:
        raise ValueError("No shards to merge")

    shard_sizes: list[int] = []
    image_size = 0
    channels = 3
    channel_order = ",".join(CHANNEL_ORDER)
    for path in ordered:
        with h5py.File(path, "r") as handle:
            n = int(handle["images"].shape[0])
            shard_sizes.append(n)
            if image_size == 0:
                image_size = int(handle.attrs["image_size"])
                channels = int(handle.attrs.get("channels", 3))
                channel_order = str(handle.attrs.get("channel_order", channel_order))

    total_n = sum(shard_sizes)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    str_dtype = h5py.string_dtype(encoding="utf-8")
    chunk_n = 256

    image_kw: dict[str, Any] = {
        "shape": (total_n, channels, image_size, image_size),
        "dtype": np.float32,
        "chunks": (chunk_n, channels, image_size, image_size),
    }
    if compression:
        image_kw["compression"] = compression
        if compression == "gzip":
            image_kw["compression_opts"] = compression_opts

    with h5py.File(output_path, "w") as out:
        images_ds = out.create_dataset("images", **image_kw)
        labels_ds = out.create_dataset("labels", shape=(total_n,), dtype=np.int64)
        stamps_ds = out.create_dataset(
            "detection_stamps",
            shape=(total_n,),
            dtype=str_dtype,
        )
        out.attrs["image_size"] = image_size
        out.attrs["channels"] = channels
        out.attrs["channel_order"] = channel_order

        offset = 0
        for path, n in zip(ordered, shard_sizes):
            with h5py.File(path, "r") as inp:
                images_ds[offset : offset + n] = inp["images"][:]
                labels_ds[offset : offset + n] = inp["labels"][:]
                stamps_ds[offset : offset + n] = inp["detection_stamps"][:]
            offset += n

    if delete_shards:
        for path in ordered:
            path.unlink(missing_ok=True)

    LOGGER.info("Merged %d shards -> %s (%d samples)", len(ordered), output_path, total_n)
    return total_n


def write_h5_split(
    output_path: Path,
    images: np.ndarray,
    labels: np.ndarray,
    stamps: list[str],
) -> None:
    """Write one split file: images (N,3,H,W), labels (N,), detection_stamps."""
    with H5SplitWriter(
        output_path,
        image_size=int(images.shape[-1]),
        compression="gzip",
    ) as writer:
        writer.append(images, labels, stamps)


class H5SplitWriter:
    """Incrementally write one CNN split HDF5 without holding all samples in RAM."""

    def __init__(
        self,
        output_path: Path,
        image_size: int,
        channels: int = 3,
        compression: str | None = None,
        compression_opts: int = 4,
    ) -> None:
        self.output_path = Path(output_path)
        self.image_size = int(image_size)
        self.channels = int(channels)
        self.compression = compression
        self.compression_opts = int(compression_opts)
        self._handle: h5py.File | None = None
        self._images: h5py.Dataset | None = None
        self._labels: h5py.Dataset | None = None
        self._stamps: h5py.Dataset | None = None
        self._str_dtype = h5py.string_dtype(encoding="utf-8")
        self.n_written = 0

    def __enter__(self) -> H5SplitWriter:
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = h5py.File(self.output_path, "w")
        h = w = self.image_size
        c = self.channels
        chunk_n = 256
        image_kw: dict[str, Any] = {
            "shape": (0, c, h, w),
            "maxshape": (None, c, h, w),
            "dtype": np.float32,
            "chunks": (chunk_n, c, h, w),
        }
        if self.compression:
            image_kw["compression"] = self.compression
            if self.compression == "gzip":
                image_kw["compression_opts"] = self.compression_opts
        self._images = self._handle.create_dataset("images", **image_kw)
        self._labels = self._handle.create_dataset(
            "labels",
            shape=(0,),
            maxshape=(None,),
            dtype=np.int64,
            chunks=(chunk_n * 4,),
        )
        self._stamps = self._handle.create_dataset(
            "detection_stamps",
            shape=(0,),
            maxshape=(None,),
            dtype=self._str_dtype,
            chunks=(chunk_n * 4,),
        )
        self._handle.attrs["image_size"] = h
        self._handle.attrs["channels"] = c
        self._handle.attrs["channel_order"] = ",".join(CHANNEL_ORDER)
        return self

    def append(
        self,
        images: np.ndarray,
        labels: np.ndarray,
        stamps: list[str],
    ) -> None:
        if self._images is None or self._labels is None or self._stamps is None:
            raise RuntimeError("H5SplitWriter is not open")
        n = int(labels.shape[0])
        if n == 0:
            return
        if images.shape[0] != n or len(stamps) != n:
            raise ValueError("images, labels, and stamps batch sizes must match")
        offset = self.n_written
        self._images.resize(offset + n, axis=0)
        self._images[offset : offset + n] = images.astype(np.float32, copy=False)
        self._labels.resize(offset + n, axis=0)
        self._labels[offset : offset + n] = labels.astype(np.int64, copy=False)
        self._stamps.resize(offset + n, axis=0)
        self._stamps[offset : offset + n] = np.array(stamps, dtype=object)
        self.n_written += n

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._handle is not None:
            n = self.n_written
            path = self.output_path
            self._handle.close()
            self._handle = None
            self._images = None
            self._labels = None
            self._stamps = None
            if exc_type is None and n > 0:
                LOGGER.info("Wrote %s (%d samples)", path, n)


class H5TripletDataset(Dataset):
    """Lazy HDF5 reader for CNN training."""

    def __init__(
        self,
        h5_path: str | Path,
        preprocess: bool = True,
        return_stamp: bool = False,
        norm_stats_path: str | Path | None = None,
    ) -> None:
        self.h5_path = str(h5_path)
        self.preprocess = preprocess
        self.return_stamp = return_stamp
        self._handle: h5py.File | None = None
        self._lowers: np.ndarray | None = None
        self._uppers: np.ndarray | None = None
        self._epoch_order: np.ndarray | None = None

        if preprocess:
            stats_path = Path(norm_stats_path) if norm_stats_path else zscale_stats_path_for_h5(h5_path)
            if stats_path.is_file():
                self._lowers, self._uppers = load_zscale_stats(stats_path)
            else:
                LOGGER.warning(
                    "No %s — using legacy fixed clip limits (run 3.Build_CNN_dataset.py to create z-scale stats)",
                    stats_path,
                )
                self._lowers, self._uppers = LOWERS, UPPERS

        with h5py.File(self.h5_path, "r") as handle:
            self.length = int(handle["images"].shape[0])

    def set_epoch(self, epoch: int, seed: int = 42, chunk_size: int = H5_IMAGE_CHUNK) -> None:
        """Shuffle sample order in HDF5-chunk blocks so reads stay mostly sequential."""
        rng = np.random.default_rng(int(seed) + int(epoch))
        n = self.length
        chunk = max(1, int(chunk_size))
        n_chunks = (n + chunk - 1) // chunk
        chunk_ids = np.arange(n_chunks)
        rng.shuffle(chunk_ids)
        blocks: list[np.ndarray] = []
        for cid in chunk_ids:
            start = int(cid) * chunk
            end = min(start + chunk, n)
            block = np.arange(start, end, dtype=np.int64)
            rng.shuffle(block)
            blocks.append(block)
        self._epoch_order = np.concatenate(blocks) if blocks else np.arange(n, dtype=np.int64)

    def _resolve_index(self, index: int) -> int:
        if self._epoch_order is not None:
            return int(self._epoch_order[int(index)])
        return int(index)

    def _open(self) -> h5py.File:
        if self._handle is None:
            self._handle = h5py.File(
                self.h5_path,
                "r",
                rdcc_nbytes=H5_RDCC_NBYTES,
                rdcc_w0=0.75,
            )
        return self._handle

    def __len__(self) -> int:
        return self.length

    def __getitem__(self, index: int):
        sample_idx = self._resolve_index(index)
        handle = self._open()
        images = handle["images"][sample_idx]
        orig_label = int(handle["labels"][sample_idx])
        binary_label = int(collapse_binary_label(orig_label))

        if self.preprocess:
            tensor = torch.from_numpy(preprocess_numpy(images, self._lowers, self._uppers))
        else:
            tensor = torch.from_numpy(images)

        # Model expects (C, H, W).
        if tensor.ndim == 3 and tensor.shape[-1] == 3:
            tensor = tensor.permute(2, 0, 1)
        elif tensor.ndim == 3 and tensor.shape[0] == 3:
            pass
        else:
            raise ValueError(f"Unexpected image shape {tuple(tensor.shape)}")

        label = torch.tensor(binary_label, dtype=torch.float32)
        orig = torch.tensor(orig_label, dtype=torch.long)

        if self.return_stamp:
            stamp = handle["detection_stamps"][sample_idx]
            if isinstance(stamp, bytes):
                stamp = stamp.decode("utf-8")
            return tensor, label, orig, stamp
        return tensor, label, orig

    def __del__(self) -> None:
        if self._handle is not None:
            self._handle.close()
