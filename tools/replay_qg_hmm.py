#!/usr/bin/env python3
"""Causally replay float and quantized QG-HMM inference without ground truth."""
import argparse
import json
import os

import numpy as np
import pandas as pd

from qghmm_common import predict_float, softmax


ID_COLUMNS = ["run_id", "timestamp_ns", "link_id", "switch_id"]


def confidence_from_margin(margin, calibration):
    maximum = max(float(calibration["margin_max"]), 1e-9)
    index = np.clip((margin / maximum * 255).astype(int), 0, 255)
    return np.asarray(calibration["entries"], dtype=int)[index]


def decisions(scores, familiarity, classes, calibration):
    order = np.argsort(scores, axis=1)
    top = order[:, -1]
    second = order[:, -2]
    margin = scores[np.arange(len(scores)), top] - scores[np.arange(len(scores)), second]
    state = np.asarray([classes[index] for index in top], dtype=object)
    low_familiarity = familiarity < float(calibration["familiarity_threshold"])
    ambiguous = (~low_familiarity &
                 (margin < float(calibration["ambiguous_margin_threshold"])))
    state[low_familiarity] = "UNKNOWN"
    state[ambiguous] = "AMBIGUOUS"
    confidence = confidence_from_margin(margin, calibration)
    familiar_score = np.clip(
        128 + 16 * (familiarity - float(calibration["familiarity_threshold"])),
        0, 255,
    ).astype(int)
    return state, np.asarray([classes[index] for index in second]), margin, confidence, familiar_score


def quantized_scores(frame, model):
    features, classes = model["features"], model["classes"]
    values = frame[features].to_numpy(dtype=float)
    q = np.zeros_like(values, dtype=np.int16)
    for index, feature in enumerate(features):
        spec = model["feature_quantization"][feature]
        clipped = np.clip(values[:, index], spec["clip_min"], spec["clip_max"])
        q[:, index] = np.clip(np.round(clipped / spec["scale"]), -128, 127).astype(np.int16)
    lut = np.asarray(model["emission_lut_int16"], dtype=np.int32)
    bias = np.asarray(model["component_bias_int16"], dtype=np.int32)
    initial_priors = np.asarray(model["initial_prior_int16"], dtype=np.int32)
    transitions = np.asarray(model["transition_int16"], dtype=np.int32)
    component_scores = np.zeros((len(frame), lut.shape[0], lut.shape[1]), dtype=np.int32)
    for class_index in range(lut.shape[0]):
        for component in range(lut.shape[1]):
            total = np.full(len(frame), int(bias[class_index, component]), dtype=np.int32)
            for feature_index in range(lut.shape[2]):
                total += lut[class_index, component, feature_index,
                             q[:, feature_index] + 128]
            component_scores[:, class_index, component] = total
    if component_scores.shape[2] == 2:
        maximum = component_scores.max(axis=2)
        difference = np.clip(np.abs(component_scores[:, :, 0]
                                    - component_scores[:, :, 1]), 0, 255)
        correction = np.asarray(model["mixture_correction_lut_int16"], dtype=np.int32)
        emissions = maximum + correction[difference]
    else:
        emissions = component_scores[:, :, 0]
    scores = np.zeros_like(emissions, dtype=np.int32)
    for _, group in frame.groupby(["run_id", "link_id"], sort=False):
        state = None
        for index in group.index:
            if state is None:
                state = emissions[index] + initial_priors
            else:
                state = emissions[index] + np.max(state[:, None] + transitions, axis=0)
            state -= state.max()
            scores[index] = state
    scale = float(model["score_scale"])
    return scores / scale, emissions.max(axis=1) / scale, q


def add_top_k(output, score_column, prefix):
    result = output.copy()
    values = {}
    for (run_id, timestamp), group in result.groupby(["run_id", "timestamp_ns"]):
        ordered = group.sort_values(score_column, ascending=False)["link_id"].tolist()[:3]
        values[(run_id, timestamp)] = ordered
    result[prefix + "top_k_links"] = [
        json.dumps(values[(row.run_id, row.timestamp_ns)]) for row in result.itertuples()
    ]
    return result


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--float-model", required=True)
    ap.add_argument("--quantized-model", required=True)
    ap.add_argument("--out-outputs", required=True)
    ap.add_argument("--out-alarms", required=True)
    ap.add_argument("--out-jsonl", required=True)
    args = ap.parse_args()
    dataset = pd.read_parquet(args.dataset) if args.dataset.endswith(".parquet") else pd.read_csv(args.dataset)
    float_model = json.load(open(args.float_model))
    quantized_model = json.load(open(args.quantized_model))
    features, classes = float_model["features"], float_model["classes"]
    # This is the inference boundary: labels, schedules, target links,
    # severity, configured bandwidth and future completion are not copied.
    frame = dataset[ID_COLUMNS + features + ["fast_link_event", "link_state_down"]].copy().sort_values(
        ["run_id", "link_id", "timestamp_ns"]).reset_index(drop=True)
    kind = float_model["selected_deployable_model"]
    float_scores, float_familiarity = predict_float(
        frame, features, classes, float_model["models"][kind], kind)
    f_state, f_second, f_margin, f_conf, f_familiar = decisions(
        float_scores, float_familiarity, classes, float_model["calibration"])
    quant_scores, quant_familiarity, quant_features = quantized_scores(frame, quantized_model)
    q_state, q_second, q_margin, q_conf, q_familiar = decisions(
        quant_scores, quant_familiarity, classes, quantized_model["calibration"])
    healthy_index = classes.index("HEALTHY")
    nonhealthy = [index for index, label in enumerate(classes) if label != "HEALTHY"]
    float_anomaly = float_scores[:, nonhealthy].max(axis=1) - float_scores[:, healthy_index]
    quant_anomaly = quant_scores[:, nonhealthy].max(axis=1) - quant_scores[:, healthy_index]
    output = frame[ID_COLUMNS].copy()
    output["float_state"] = f_state
    output["float_second_candidate"] = f_second
    output["float_class_confidence"] = f_conf
    output["float_familiarity"] = f_familiar
    output["float_margin"] = f_margin
    output["float_anomaly_score"] = float_anomaly
    output["state"] = q_state
    output["second_candidate"] = q_second
    output["class_confidence"] = q_conf
    output["familiarity"] = q_familiar
    output["uncertainty"] = 255 - q_conf
    output["margin"] = q_margin
    output["anomaly_score"] = quant_anomaly
    probabilities = softmax(quant_scores)
    for class_index, label in enumerate(classes):
        output[f"prob_{label}"] = probabilities[:, class_index]
    severity_signal = np.maximum.reduce([
        np.maximum(0, -frame["tx_rate_residual"].to_numpy()),
        np.maximum(0, frame["queue_peak_residual"].to_numpy()),
        np.maximum(0, frame["drop_error_delta"].to_numpy()),
    ])
    output["severity_bin"] = np.digitize(severity_signal, [1.0, 2.5, 5.0]).astype(int)
    # Explicit physical state transitions bypass the learned gray-fault model.
    down = frame["link_state_down"].to_numpy(dtype=int) > 0
    flap = (frame["fast_link_event"].to_numpy(dtype=int) > 0) & ~down
    output.loc[down, "state"] = "LINK_DOWN"
    output.loc[flap, "state"] = "LINK_FLAP"
    output.loc[down | flap, "class_confidence"] = 255
    output.loc[down | flap, "uncertainty"] = 0
    output = add_top_k(output, "anomaly_score", "")
    os.makedirs(os.path.dirname(args.out_outputs) or ".", exist_ok=True)
    output.to_csv(args.out_outputs, index=False)
    previous_state = output.groupby(["run_id", "link_id"])["state"].shift(fill_value="HEALTHY")
    alarm_mask = output["state"].ne("HEALTHY") & output["state"].ne(previous_state)
    alarms = output[alarm_mask].copy()
    alarms.to_csv(args.out_alarms, index=False)
    with open(args.out_jsonl, "w") as out:
        for row in alarms.to_dict("records"):
            row["top_k_links"] = json.loads(row["top_k_links"])
            out.write(json.dumps(row) + "\n")
    print(json.dumps({
        "rows": len(output), "alarms_retained": len(alarms),
        "float_quantized_state_agreement": float((f_state == q_state).mean()),
        "ground_truth_columns_read_by_inference": [],
    }, indent=2))


if __name__ == "__main__":
    main()
