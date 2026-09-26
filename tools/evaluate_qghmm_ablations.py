#!/usr/bin/env python3
"""Run the six required QG-HMM ablations on locked train/test run splits."""
import argparse
import json
import os

import numpy as np
import pandas as pd

from qghmm_common import (
    class_predictions, learn_transitions, macro_f1, predict_float, softmax,
    weighted_gaussians,
)


RAW_FEATURES = [
    "tx_rate_log", "rx_rate_log", "asymmetry_raw", "queue_ewma_log",
    "queue_peak_log", "queue_growth_raw", "drop_error_log", "peer_gap_raw",
]


def train_m2(train, features, classes, variance_floor):
    x = train[features].to_numpy(dtype=float)
    y = train["fault_class"].to_numpy()
    weights = train["sample_weight"].to_numpy(dtype=float)
    emission = weighted_gaussians(x, y, weights, classes, variance_floor)
    transition = learn_transitions(train, classes)
    return {"emission": emission, "log_priors": emission["log_priors"], **transition}


def evaluate_model(test, features, classes, model, kind="M2"):
    scores, familiarity = predict_float(test, features, classes, model, kind)
    predicted, _ = class_predictions(scores, classes)
    return {"macro_f1": macro_f1(test["fault_class"].to_numpy(), predicted, classes),
            "predicted": predicted, "scores": scores, "familiarity": familiarity}


def ece(confidence, correct, bins=10):
    total = 0.0
    for index in range(bins):
        lo, hi = index / bins, (index + 1) / bins
        mask = (confidence >= lo) & (confidence <= hi if index == bins - 1 else confidence < hi)
        if mask.any():
            total += mask.mean() * abs(confidence[mask].mean() - correct[mask].mean())
    return float(total)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--float-model", required=True)
    ap.add_argument("--outputs", required=True)
    ap.add_argument("--out-json", required=True)
    args = ap.parse_args()
    frame = pd.read_parquet(args.dataset) if args.dataset.endswith(".parquet") else pd.read_csv(args.dataset)
    artifact = json.load(open(args.float_model))
    outputs = pd.read_csv(args.outputs)
    features, classes = artifact["features"], artifact["classes"]
    train = frame[(frame["split"] == "train") & frame["fault_class"].isin(classes)]
    test = frame[(frame["split"] == "test") & frame["fault_class"].isin(classes)]
    variance_floor = artifact["models"]["M2"]["emission"]["variance_floor"]
    selected = artifact["selected_deployable_model"]
    full = evaluate_model(test, features, classes, artifact["models"][selected], selected)
    no_context_model = train_m2(train, RAW_FEATURES, classes, variance_floor)
    a1 = evaluate_model(test, RAW_FEATURES, classes, no_context_model)
    a2 = evaluate_model(test, features, classes, artifact["models"]["M1"], "M1")
    no_peak = [name for name in features if name != "queue_peak_residual"]
    a3 = evaluate_model(test, no_peak, classes,
                        train_m2(train, no_peak, classes, variance_floor))
    no_peer = [name for name in features if name != "peer_rate_gap"]
    a4 = evaluate_model(test, no_peer, classes,
                        train_m2(train, no_peer, classes, variance_floor))
    merged = outputs.merge(frame[["run_id", "timestamp_ns", "link_id", "switch_id",
                                  "split", "fault_class", "ood_kind"]],
                           on=["run_id", "timestamp_ns", "link_id", "switch_id"])
    ood_test = merged[merged["split"] == "test"]
    ood_eval = ((ood_test["fault_class"] == "UNKNOWN")
                & ~ood_test["ood_kind"].isin(["link_flap", "link_down"]))
    a5_unknown_recall = float((ood_test.loc[ood_eval,
                                                "float_state"] == "UNKNOWN").mean()) \
        if ood_eval.any() else None
    probabilities = softmax(full["scores"])
    confidence = probabilities.max(axis=1)
    a6_ece = ece(confidence, full["predicted"] == test["fault_class"].to_numpy())
    result = {
        "full_model": {"macro_f1": full["macro_f1"]},
        "A1_without_context_normalization": {"macro_f1": a1["macro_f1"],
                                              "delta": a1["macro_f1"] - full["macro_f1"]},
        "A2_without_hmm_transition": {"macro_f1": a2["macro_f1"],
                                      "delta": a2["macro_f1"] - full["macro_f1"]},
        "A3_without_queue_peak": {"macro_f1": a3["macro_f1"],
                                  "delta": a3["macro_f1"] - full["macro_f1"]},
        "A4_without_peer_rate_gap": {"macro_f1": a4["macro_f1"],
                                     "delta": a4["macro_f1"] - full["macro_f1"]},
        "A5_without_unknown_rejection": {"unknown_recall": 0.0,
                                         "full_unknown_recall": a5_unknown_recall},
        "A6_without_confidence_calibration": {"uncalibrated_ece": a6_ece},
        "split_policy": "all ablations reuse locked run-level train/test splits",
    }
    os.makedirs(os.path.dirname(args.out_json) or ".", exist_ok=True)
    with open(args.out_json, "w") as out:
        json.dump(result, out, indent=2)
        out.write("\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
