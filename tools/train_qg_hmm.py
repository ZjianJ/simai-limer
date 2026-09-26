#!/usr/bin/env python3
"""Train M1/M2/M3 plus a sparse-RBF offline upper bound without label leakage."""
import argparse
import json
import os

import numpy as np
import pandas as pd

from qghmm_common import (
    class_predictions, gaussian_emissions, learn_transitions, macro_f1,
    predict_float, softmax, train_gmm, train_sparse_rbf, weighted_gaussians,
)


def confidence_lut(margins, correct, entries=256):
    maximum = max(float(np.quantile(margins, 0.99)), 1e-6)
    bins = np.clip((margins / maximum * (entries - 1)).astype(int), 0, entries - 1)
    global_accuracy = float(correct.mean()) if len(correct) else 0.0
    values = np.full(entries, global_accuracy)
    for index in range(entries):
        neighborhood = np.abs(bins - index) <= 4
        if neighborhood.sum() >= 8:
            values[index] = float(correct[neighborhood].mean())
    values = np.maximum.accumulate(values)
    return {"entries": np.clip(np.round(values * 255), 0, 255).astype(int).tolist(),
            "margin_max": maximum, "mapping": "linear_clip_then_lookup"}


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--dataset-checks", required=True)
    ap.add_argument("--out-model", required=True)
    ap.add_argument("--variance-floor", type=float, default=0.05)
    ap.add_argument("--m3-min-improvement", type=float, default=0.02)
    ap.add_argument("--seed", type=int, default=2026)
    args = ap.parse_args()
    checks = json.load(open(args.dataset_checks))
    if not all(item["pass"] for item in checks["checks"].values()):
        raise SystemExit("dataset checks did not pass; refusing to train")
    frame = pd.read_parquet(args.dataset) if args.dataset.endswith(".parquet") else pd.read_csv(args.dataset)
    features = checks["features"]
    forbidden = {"fault_class", "fault_id", "severity", "fault_coverage_ratio",
                 "configured_bandwidth_bps", "target_link_id"}
    if forbidden.intersection(features):
        raise SystemExit(f"ground-truth or forbidden inference feature: {forbidden.intersection(features)}")
    train = frame[(frame["split"] == "train") & (frame["fault_class"] != "UNKNOWN")].copy()
    validation = frame[(frame["split"] == "validation") &
                       (frame["fault_class"] != "UNKNOWN")].copy()
    classes = sorted(train["fault_class"].unique())
    required = {"HEALTHY", "ACCESS_FAIL_SLOW", "CONGESTION_HOTSPOT"}
    if not required.issubset(classes):
        raise SystemExit(f"first-round classes missing from training: {sorted(required - set(classes))}")
    x = train[features].to_numpy(dtype=float)
    y = train["fault_class"].to_numpy()
    weights = train["sample_weight"].to_numpy(dtype=float)
    emission = weighted_gaussians(x, y, weights, classes, args.variance_floor)
    transitions = learn_transitions(train, classes)
    m1 = {"emission": emission, "log_priors": emission["log_priors"]}
    m2 = {"emission": emission, "log_priors": emission["log_priors"], **transitions}
    gmm = train_gmm(x, y, weights, classes, components=2,
                    variance_floor=args.variance_floor, seed=args.seed)
    m3 = {"emission": gmm, "log_priors": emission["log_priors"], **transitions}
    m4 = train_sparse_rbf(x, y, weights, classes, seed=args.seed)
    models = {"M1": m1, "M2": m2, "M3": m3, "M4": m4}
    validation_scores = {}
    validation_f1 = {}
    for name, model in models.items():
        scores, familiarity = predict_float(validation, features, classes, model, name)
        predicted, _ = class_predictions(scores, classes)
        validation_scores[name] = (scores, familiarity, predicted)
        validation_f1[name] = macro_f1(validation["fault_class"].to_numpy(), predicted, classes)
    selected = "M3" if validation_f1["M3"] >= validation_f1["M2"] + args.m3_min_improvement else "M2"
    scores, familiarity, predicted = validation_scores[selected]
    ordered = np.sort(scores, axis=1)
    margins = ordered[:, -1] - ordered[:, -2]
    correct = predicted == validation["fault_class"].to_numpy()
    calibration = confidence_lut(margins, correct)
    calibration["familiarity_threshold"] = float(np.quantile(familiarity, 0.01))
    correct_margins = margins[correct]
    calibration["ambiguous_margin_threshold"] = float(
        np.quantile(correct_margins, 0.05) if len(correct_margins) else 0.0)
    probabilities = softmax(scores)
    true_indices = np.asarray([classes.index(label) for label in validation["fault_class"]])
    calibration["validation_nll_uncalibrated"] = float(
        -np.log(probabilities[np.arange(len(probabilities)), true_indices] + 1e-30).mean())
    artifact = {
        "schema_version": 1,
        "features": features,
        "classes": classes,
        "models": models,
        "selected_deployable_model": selected,
        "selection_rule": f"M3 only if validation macro-F1 improves by >= {args.m3_min_improvement}",
        "validation_macro_f1": validation_f1,
        "calibration": calibration,
        "training_rows": len(train),
        "validation_rows": len(validation),
        "forbidden_inference_fields": sorted(forbidden),
        "m0_reference": "switch_sparse from compare_detection_baselines.py",
    }
    os.makedirs(os.path.dirname(args.out_model) or ".", exist_ok=True)
    with open(args.out_model, "w") as out:
        json.dump(artifact, out, indent=2)
        out.write("\n")
    print(json.dumps({"selected": selected, "validation_macro_f1": validation_f1,
                      "classes": classes}, indent=2))


if __name__ == "__main__":
    main()
