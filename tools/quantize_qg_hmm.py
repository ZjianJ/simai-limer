#!/usr/bin/env python3
"""Quantize the selected Gaussian HMM into int8 features and int16 LUT scores."""
import argparse
import hashlib
import json
import os

import numpy as np
import pandas as pd


def canonical_bytes(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--float-model", required=True)
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--out-model", required=True)
    ap.add_argument("--out-calibration", required=True)
    ap.add_argument("--score-scale", type=float, default=32.0)
    args = ap.parse_args()
    artifact = json.load(open(args.float_model))
    frame = pd.read_parquet(args.dataset) if args.dataset.endswith(".parquet") else pd.read_csv(args.dataset)
    train = frame[frame["split"] == "train"]
    features, classes = artifact["features"], artifact["classes"]
    selected = artifact["selected_deployable_model"]
    if selected not in {"M2", "M3"}:
        raise SystemExit(f"selected model {selected} is not a deployable HMM")
    clip_abs = []
    for feature in features:
        # A global percentile is dominated by HEALTHY rows and can erase a
        # rare class completely (drop/error deltas are zero almost always).
        # Derive the range from training data only, but balance classes by
        # taking the largest within-class 99.5th percentile.
        value = max(
            float(np.quantile(np.abs(train.loc[
                train["fault_class"] == label, feature].to_numpy(dtype=float)), 0.995))
            for label in classes
        )
        clip_abs.append(max(value, 0.25))
    qvalues = np.arange(-128, 128, dtype=float)
    dequantized = [qvalues / 127.0 * value for value in clip_abs]
    model = artifact["models"][selected]
    emission = model["emission"]
    if selected == "M2":
        means = np.asarray(emission["means"], dtype=float)[:, None, :]
        variances = np.asarray(emission["variances"], dtype=float)[:, None, :]
        log_mixture = np.zeros((len(classes), 1))
        components = 1
    else:
        means = np.asarray(emission["means"], dtype=float)
        variances = np.asarray(emission["variances"], dtype=float)
        log_mixture = np.asarray(emission["log_mixture"], dtype=float)
        components = int(emission["components"])
    lut = np.zeros((len(classes), components, len(features), 256), dtype=np.int16)
    for class_index in range(len(classes)):
        for component in range(components):
            for feature_index, values in enumerate(dequantized):
                contribution = -0.5 * (
                    np.log(2 * np.pi * variances[class_index, component, feature_index])
                    + (values - means[class_index, component, feature_index]) ** 2
                    / variances[class_index, component, feature_index]
                )
                lut[class_index, component, feature_index] = np.clip(
                    np.round(contribution * args.score_scale), -32768, 32767
                ).astype(np.int16)
    priors = np.asarray(model["log_priors"], dtype=float)
    # Mixture weights are part of every emission. Class priors are not: the
    # Viterbi recurrence applies them only when a run/link sequence starts.
    biases = np.round(log_mixture * args.score_scale).astype(np.int16)
    initial_priors = np.clip(
        np.round(priors * args.score_scale), -32768, 32767
    ).astype(np.int16)
    transitions = np.clip(
        np.round(np.asarray(model["log_transition"]) * args.score_scale),
        -32768, 32767,
    ).astype(np.int16)
    # For two components, logsumexp(a,b) = max(a,b) +
    # log1p(exp(-abs(a-b))). The correction is indexed by an integer score
    # difference, so online inference still uses only lookup/add/max/compare.
    mixture_correction = np.round(
        np.log1p(np.exp(-np.arange(256, dtype=float) / args.score_scale))
        * args.score_scale
    ).astype(np.int16)
    state_layout = {
        "last_timestamp_ns": 8,
        "last_tx_bytes": 8,
        "last_rx_bytes": 8,
        "queue_ewma_and_previous_queue": 8,
        "previous_drop_error_and_flags": 8,
        "hmm_scores_int32": 4 * len(classes),
        "context_and_padding": 64 - (40 + 4 * len(classes)),
    }
    if state_layout["context_and_padding"] < 0:
        raise SystemExit("class scores exceed 64-byte per-port state budget")
    quantized = {
        "schema_version": 1,
        "model_kind": selected,
        "features": features,
        "classes": classes,
        "feature_quantization": {
            feature: {"clip_min": -clip_abs[index], "clip_max": clip_abs[index],
                      "dtype": "int8", "scale": clip_abs[index] / 127.0}
            for index, feature in enumerate(features)
        },
        "feature_clip_policy": "max per-training-class absolute p99.5",
        "score_scale": args.score_scale,
        "emission_lut_int16": lut.astype(int).tolist(),
        "component_bias_int16": biases.astype(int).tolist(),
        "initial_prior_int16": initial_priors.astype(int).tolist(),
        "transition_int16": transitions.astype(int).tolist(),
        "mixture_reduction": ("logsumexp_correction_lut"
                              if components > 1 else "single_component"),
        "mixture_correction_lut_int16": (
            mixture_correction.astype(int).tolist() if components > 1 else []),
        "calibration": artifact["calibration"],
        "resource_budget": {
            "dynamic_state_bytes_per_port": 64,
            "dynamic_state_layout": state_layout,
            "shared_model_bytes": int(lut.nbytes + biases.nbytes + initial_priors.nbytes
                                      + transitions.nbytes
                                      + (mixture_correction.nbytes if components > 1 else 0)),
            "lut_entries": int(lut.size + (len(mixture_correction)
                                             if components > 1 else 0)),
            "lookups_per_inference": int(len(classes) * components * len(features)
                                         + (len(classes) if components > 1 else 0)),
            "integer_additions_per_inference": int(
                len(classes) * components * max(0, len(features) - 1)
                + len(classes) * len(classes)
                + (len(classes) if components > 1 else 0)),
            "comparisons_per_inference": int(
                len(classes) * max(0, components - 1)
                + len(classes) * len(classes) + len(classes) - 1),
        },
    }
    quantized["deterministic_sha256"] = hashlib.sha256(canonical_bytes(quantized)).hexdigest()
    os.makedirs(os.path.dirname(args.out_model) or ".", exist_ok=True)
    with open(args.out_model, "w") as out:
        json.dump(quantized, out, indent=2)
        out.write("\n")
    with open(args.out_calibration, "w") as out:
        json.dump(artifact["calibration"], out, indent=2)
        out.write("\n")
    print(json.dumps({
        "model_kind": selected,
        "resource_budget": quantized["resource_budget"],
        "deterministic_sha256": quantized["deterministic_sha256"],
    }, indent=2))


if __name__ == "__main__":
    main()
