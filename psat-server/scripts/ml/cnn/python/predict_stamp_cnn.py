#!/usr/bin/env python3
"""
Score a single Pan-STARRS stamp triplet with the PyTorch CNN.

Useful for quick checks without the database pipeline.

Example:
  python predict_stamp_cnn.py --model cnn_models/cnn_model_ps1.pt --camera PS1 \\
      --diff /storage2/images/ps13pi/60301/1074146861404330000_60301.434_46883653_856_diff.fits
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from cnn_inference import load_pytorch_classifier, predict_triplet_probabilities


def main() -> None:
    parser = argparse.ArgumentParser(description="Score one Pan-STARRS FITS triplet.")
    parser.add_argument("--model", required=True, help="Path to cnn_model_ps1.pt or cnn_model_ps2.pt")
    parser.add_argument("--camera", required=True, choices=["PS1", "PS2"], help="Camera for z-scale stats")
    parser.add_argument("--diff", required=True, help="Diff FITS path or DB-style basename.fits")
    parser.add_argument("--zscale-stats", default=None, help="Override zscale_stats.json path")
    parser.add_argument("--data-root", default=None, help="CNN data root (parent of PS1/PS2 folders)")
    parser.add_argument("--json", action="store_true", help="Print JSON instead of plain text")
    args = parser.parse_args()

    bundle = load_pytorch_classifier(
        args.model,
        args.camera,
        zscale_stats_path=args.zscale_stats,
        data_root=args.data_root,
    )
    used_paths, probs = predict_triplet_probabilities([args.diff], bundle)
    if not len(probs):
        print("Could not load triplet for:", args.diff, file=sys.stderr)
        sys.exit(1)

    prob = float(probs[0])
    label = int(prob >= bundle.decision_threshold)
    payload = {
        "diff_path": used_paths[0],
        "probability_real": prob,
        "decision_threshold": bundle.decision_threshold,
        "predicted_label": label,
        "camera": bundle.camera,
        "model": str(Path(args.model).resolve()),
    }
    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        print(f"probability_real={prob:.6f}")
        print(f"decision_threshold={bundle.decision_threshold:.6f}")
        print(f"predicted_label={label}  (0=garbage, 1=real)")


if __name__ == "__main__":
    main()
