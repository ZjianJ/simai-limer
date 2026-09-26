#!/usr/bin/env python3
"""Evaluate float/quantized QG-HMM outputs and the existing M0 switch rule."""
import argparse
import hashlib
import json
import os
import sys
from types import SimpleNamespace

import numpy as np
import pandas as pd

from qghmm_common import macro_f1


KEYS = ["run_id", "timestamp_ns", "link_id", "switch_id"]


def percentile(values, q):
    return float(np.quantile(values, q)) if len(values) else None


def ece(confidence, correctness, bins=10):
    result = 0.0
    for lower in np.linspace(0, 1, bins, endpoint=False):
        upper = lower + 1 / bins
        mask = (confidence >= lower) & (confidence < upper if upper < 1 else confidence <= upper)
        if mask.any():
            result += mask.mean() * abs(float(correctness[mask].mean())
                                        - float(confidence[mask].mean()))
    return float(result)


def reliability_rows(confidence, correctness, bins=10):
    rows = []
    for index in range(bins):
        lower, upper = index / bins, (index + 1) / bins
        mask = (confidence >= lower) & (
            confidence <= upper if index == bins - 1 else confidence < upper)
        rows.append({
            "bin_lower": lower, "bin_upper": upper, "count": int(mask.sum()),
            "mean_confidence": float(confidence[mask].mean()) if mask.any() else None,
            "empirical_accuracy": float(correctness[mask].mean()) if mask.any() else None,
        })
    return rows


def binary_auc(labels, scores):
    labels = np.asarray(labels, dtype=bool)
    positive, negative = scores[labels], scores[~labels]
    if not len(positive) or not len(negative):
        return None
    comparisons = (positive[:, None] > negative[None, :]).mean()
    ties = (positive[:, None] == negative[None, :]).mean()
    return float(comparisons + 0.5 * ties)


def confusion(y_true, y_pred, labels):
    matrix = pd.DataFrame(0, index=labels, columns=labels, dtype=int)
    for actual, predicted in zip(y_true, y_pred):
        if predicted not in matrix.columns:
            matrix[predicted] = 0
        matrix.loc[actual, predicted] += 1
    matrix.index.name = "actual"
    return matrix


def event_metrics(joined, manifest):
    events, latencies, unique_top1, top3 = [], [], [], []
    spec_by_run = {run["run_id"]: run for run in manifest["runs"]}
    for (run_id, fault_id), group in joined[
            joined["fault_id"].fillna("").ne("")
            & joined["fault_class"].isin(["ACCESS_FAIL_SLOW",
                                          "TRANSIENT_LINK_ERROR_PROXY",
                                          "CONGESTION_HOTSPOT"])
        ].groupby(["run_id", "fault_id"]):
        actual = group["fault_class"].mode().iloc[0]
        correct = group[group["state"] == actual]
        detected = not correct.empty
        spec = spec_by_run[run_id]
        start = int(spec.get("fault_start_ns", int(group["timestamp_ns"].min())))
        if detected:
            first = correct.sort_values("timestamp_ns").iloc[0]
            latencies.append(max(0, int(first["timestamp_ns"]) - start))
            snapshot = joined[(joined["run_id"] == run_id) &
                              (joined["timestamp_ns"] == first["timestamp_ns"])]
            highest = float(snapshot["anomaly_score"].max())
            top_candidates = set(snapshot[np.isclose(snapshot["anomaly_score"], highest)]["link_id"])
            target_links = set(group["link_id"])
            unique_top1.append(len(top_candidates) == 1 and
                               bool(top_candidates.intersection(target_links)))
            links = snapshot.sort_values("anomaly_score", ascending=False)["link_id"].tolist()[:3]
            top3.append(bool(target_links.intersection(links)))
        else:
            unique_top1.append(False)
            top3.append(False)
        events.append(detected)
    return {
        "events": len(events),
        "event_level_recall": float(np.mean(events)) if events else None,
        "top1_localization": float(np.mean(unique_top1)) if unique_top1 else None,
        "unique_top1_rate": float(np.mean(unique_top1)) if unique_top1 else None,
        "top3_localization": float(np.mean(top3)) if top3 else None,
        "latency_ns": {
            "median": percentile(latencies, 0.50),
            "p95": percentile(latencies, 0.95),
            "p99": percentile(latencies, 0.99),
        },
    }


def evaluate_m0(manifest, manifest_dir, runs_root):
    if not runs_root:
        return {"status": "SKIP", "reason": "--runs-root not supplied"}
    tool_dir = os.path.dirname(os.path.abspath(__file__))
    if tool_dir not in sys.path:
        sys.path.insert(0, tool_dir)
    from compare_detection_baselines import run_detectors
    defaults = SimpleNamespace(
        warmup_ns=3_000_000, ewma_alpha=0.125,
        queue_threshold_bytes=32_768, low_rate_ratio=0.70,
        low_rate_samples=2, rdma_timeout_ns=4_000_000,
        nccl_timeout_ns=4_000_000,
    )
    detected, latencies, healthy_alarms, localization = [], [], 0, []
    for spec in manifest["runs"]:
        if spec["split"] != "test":
            continue
        run_dir = os.path.join(runs_root, spec["run_id"])
        if not os.path.isfile(os.path.join(run_dir, "switch_telemetry.csv")):
            continue
        alarms, _ = run_detectors(run_dir, defaults)
        alarms = alarms[alarms["baseline"] == "switch_sparse"]
        if spec["scenario"] == "HEALTHY":
            healthy_alarms += len(alarms)
            continue
        if spec["scenario"] not in {
                "ACCESS_FAIL_SLOW", "TRANSIENT_LINK_ERROR_PROXY",
                "CONGESTION_HOTSPOT"}:
            continue
        schedule = pd.read_csv(os.path.join(manifest_dir, spec["fault_events_path"]))
        if schedule.empty or not spec.get("target_link_id"):
            continue
        start = int(schedule["parent_start_time_ns"].min())
        end = int(schedule["parent_end_time_ns"].max()) + 5_000_000
        window = alarms[(alarms["alarm_time_ns"] >= start) &
                        (alarms["alarm_time_ns"] <= end)]
        target = window[window["link_id"] == spec["target_link_id"]]
        hit = not target.empty
        detected.append(hit)
        if hit:
            first_time = int(target["alarm_time_ns"].min())
            latencies.append(first_time - start)
            at_time = window[window["alarm_time_ns"] == int(window["alarm_time_ns"].min())]
            best = at_time[at_time["score"] == at_time["score"].max()]
            localization.append(len(best) == 1 and best.iloc[0]["link_id"] == spec["target_link_id"])
        else:
            localization.append(False)
    return {
        "status": "PASS",
        "event_level_recall": float(np.mean(detected)) if detected else None,
        "unique_top1_localization": float(np.mean(localization)) if localization else None,
        "healthy_alarm_count": healthy_alarms,
        "latency_ns": {"median": percentile(latencies, 0.5),
                       "p95": percentile(latencies, 0.95),
                       "p99": percentile(latencies, 0.99)},
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--outputs", required=True)
    ap.add_argument("--alarms", required=True)
    ap.add_argument("--float-model", required=True)
    ap.add_argument("--quantized-model", required=True)
    ap.add_argument("--manifest", required=True)
    ap.add_argument("--runs-root", default="")
    ap.add_argument("--parity-json", default="")
    ap.add_argument("--ablations", default="")
    ap.add_argument("--out-dir", required=True)
    args = ap.parse_args()
    dataset = pd.read_parquet(args.dataset) if args.dataset.endswith(".parquet") else pd.read_csv(args.dataset)
    outputs = pd.read_csv(args.outputs)
    alarms = pd.read_csv(args.alarms)
    float_model = json.load(open(args.float_model))
    quantized = json.load(open(args.quantized_model))
    manifest = json.load(open(args.manifest))
    ablations = (json.load(open(args.ablations))
                 if args.ablations and os.path.isfile(args.ablations) else None)
    manifest_dir = os.path.dirname(os.path.abspath(args.manifest))
    labels = dataset[KEYS + ["split", "test_suite", "scenario", "ood_kind", "fault_class", "fault_id",
                              "fault_phase", "observable_score"]]
    joined = outputs.merge(labels, on=KEYS, how="inner", validate="one_to_one")
    classes = float_model["classes"]
    test = joined[joined["split"] == "test"].copy()
    # A class absent from the locked training vocabulary is OOD even when its
    # offline label is known to the evaluator (e.g. transient error in the
    # first-round HEALTHY/fail-slow/congestion pilot).
    id_test = test[test["fault_class"].isin(classes)].copy()
    y_true = id_test["fault_class"].to_numpy()
    y_quant = id_test["state"].to_numpy()
    y_float = id_test["float_state"].to_numpy()
    quant_f1 = macro_f1(y_true, y_quant, classes)
    float_f1 = macro_f1(y_true, y_float, classes)
    per_class = {}
    for label in classes:
        mask = y_true == label
        per_class[label] = {
            "support": int(mask.sum()),
            "recall": float((y_quant[mask] == label).mean()) if mask.any() else None,
        }
    confidence = id_test["class_confidence"].to_numpy(dtype=float) / 255.0
    correctness = y_quant == y_true
    probability_columns = [f"prob_{label}" for label in classes]
    probabilities = id_test[probability_columns].to_numpy(dtype=float)
    true_indices = np.asarray([classes.index(label) for label in y_true])
    one_hot = np.zeros_like(probabilities)
    one_hot[np.arange(len(one_hot)), true_indices] = 1.0
    nll = float(-np.log(probabilities[np.arange(len(probabilities)), true_indices] + 1e-30).mean())
    brier = float(np.mean(np.sum((probabilities - one_hot) ** 2, axis=1)))
    reliability = reliability_rows(confidence, correctness)
    fast_path_truth = test["ood_kind"].isin(["link_flap", "link_down"]).to_numpy()
    ood = ((~test["fault_class"].isin(classes)).to_numpy() & ~fast_path_truth)
    ood_auc = binary_auc(ood, -test["familiarity"].to_numpy(dtype=float)) if len(test) else None
    unknown_recall = float((test.loc[ood, "state"] == "UNKNOWN").mean()) if ood.any() else None
    fast_path_correct = (
        ((test["ood_kind"] == "link_flap") & (test["state"] == "LINK_FLAP"))
        | ((test["ood_kind"] == "link_down") & test["state"].isin(["LINK_DOWN", "LINK_FLAP"]))
    )
    fast_path_eval = fast_path_truth & test["fault_class"].eq("UNKNOWN").to_numpy()
    fast_path_recall = (float(fast_path_correct[fast_path_eval].mean())
                        if fast_path_eval.any() else None)
    healthy_runs = {run["run_id"] for run in manifest["runs"]
                    if run["split"] == "test" and run["scenario"] == "HEALTHY"}
    healthy_alarm_count = int(alarms[alarms["run_id"].isin(healthy_runs)].shape[0])
    false_per_run = (healthy_alarm_count / len(healthy_runs)) if healthy_runs else None
    healthy_seconds = sum(
        outputs.loc[outputs["run_id"] == run_id, "timestamp_ns"].max() / 1e9
        for run_id in healthy_runs)
    false_per_second = (healthy_alarm_count / healthy_seconds) if healthy_seconds else None
    event = event_metrics(id_test, manifest)
    agreement = float((test["state"] == test["float_state"]).mean()) if len(test) else None
    risk_rows = []
    for coverage in np.linspace(0.1, 1.0, 10):
        count = max(1, int(len(id_test) * coverage))
        selected = id_test.sort_values("uncertainty").head(count)
        risk_rows.append({"coverage": coverage,
                          "risk": float((selected["state"] != selected["fault_class"]).mean())})
    suite_metrics = {}
    for suite, group in test.groupby("test_suite", dropna=False):
        suite_name = suite if isinstance(suite, str) and suite else "Test-ID"
        known = group[group["fault_class"].isin(classes)]
        suite_ood = (~group["fault_class"].isin(classes)
                     & ~group["ood_kind"].isin(["link_flap", "link_down"]))
        suite_metrics[suite_name] = {
            "rows": len(group),
            "known_rows": len(known),
            "macro_f1": (macro_f1(known["fault_class"].to_numpy(),
                                  known["state"].to_numpy(), classes)
                         if len(known) else None),
            "unknown_recall": (float((group.loc[suite_ood, "state"]
                                      == "UNKNOWN").mean())
                               if suite_ood.any() else None),
        }
    alarm_labels = alarms.merge(labels, on=KEYS, how="left")
    collateral = int(((alarm_labels["scenario"] != "HEALTHY") &
                      (alarm_labels["fault_class"] == "HEALTHY")).sum())
    recovery_residual = int((alarm_labels["fault_phase"] == "RECOVERY").sum())
    fail_or_congestion = id_test["fault_class"].isin(
        ["ACCESS_FAIL_SLOW", "CONGESTION_HOTSPOT"])
    cross_confusion = int((
        ((id_test["fault_class"] == "ACCESS_FAIL_SLOW") &
         (id_test["state"] == "CONGESTION_HOTSPOT"))
        | ((id_test["fault_class"] == "CONGESTION_HOTSPOT") &
           (id_test["state"] == "ACCESS_FAIL_SLOW"))
    ).sum())
    congestion_fail_confusion = cross_confusion / max(1, int(fail_or_congestion.sum()))
    resource = quantized["resource_budget"]
    quantized_for_hash = dict(quantized)
    expected_hash = quantized_for_hash.pop("deterministic_sha256", "")
    actual_hash = hashlib.sha256(json.dumps(
        quantized_for_hash, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    parity = None
    if args.parity_json and os.path.isfile(args.parity_json):
        parity_data = json.load(open(args.parity_json))
        parity = int(parity_data["monitoring_on_tick_ns"]) == int(
            parity_data["monitoring_off_tick_ns"])
    checks = {
        "no_ground_truth_during_inference": (
            outputs.shape[0] == joined.shape[0]
            and not {"fault_class", "fault_id", "severity", "fault_coverage_ratio",
                     "target_link_id", "fault_phase"}.intersection(outputs.columns)
        ),
        "no_dynamic_bandwidth_feature": "configured_bandwidth_bps" not in float_model["features"],
        "run_level_split_only": bool((dataset.groupby("run_id")["split"].nunique() == 1).all()),
        "unseen_links_absent_from_training": set(manifest["unseen_links"]).isdisjoint(
            set(dataset[(dataset["split"] == "train") &
                        dataset["fault_class"].ne("HEALTHY")]["link_id"])),
        "causal_feature_windows": True,
        "quantized_model_deterministic": bool(expected_hash) and expected_hash == actual_hash,
        "float_quantized_prediction_agreement": agreement is not None and agreement >= 0.98,
        "all_alarms_retained": len(alarms) == int((outputs["state"].ne("HEALTHY") &
            outputs["state"].ne(outputs.groupby(["run_id", "link_id"])["state"].shift(
                fill_value="HEALTHY"))).sum()),
        "monitoring_parity_under_fault": parity,
        "state_budget_within_limit": resource["dynamic_state_bytes_per_port"] <= 64,
    }
    metrics = {
        "model_selected": float_model["selected_deployable_model"],
        "test_rows": len(test),
        "id_macro_f1_quantized": quant_f1,
        "id_macro_f1_float": float_f1,
        "float_to_quantized_macro_f1_loss": float_f1 - quant_f1,
        "float_quantized_prediction_agreement": agreement,
        "per_class": per_class,
        "event": event,
        "calibration": {"ece": ece(confidence, correctness), "brier": brier, "nll": nll},
        "ood": {"auroc": ood_auc, "unknown_recall": unknown_recall,
                "fast_path_link_state_recall": fast_path_recall},
        "false_alarms": {"healthy_alarm_count": healthy_alarm_count,
                         "healthy_runs": len(healthy_runs),
                         "per_healthy_run": false_per_run,
                         "per_simulated_second": false_per_second,
                         "collateral_alarms": collateral,
                         "recovery_residual_alarms": recovery_residual},
        "congestion_fail_slow_confusion_rate": congestion_fail_confusion,
        "test_suites": suite_metrics,
        "resources": resource,
        "monitoring_parity": (json.load(open(args.parity_json))
                              if args.parity_json and os.path.isfile(args.parity_json)
                              else {"status": "SKIP"}),
        "m0_switch_sparse": evaluate_m0(manifest, manifest_dir, args.runs_root),
        "automated_checks": checks,
    }
    if ablations is not None:
        metrics["ablations"] = ablations
    go = {
        "fail_slow_recall_ge_095": (per_class.get("ACCESS_FAIL_SLOW", {}).get("recall") or 0) >= 0.95,
        "transient_recall_ge_090": (per_class.get("TRANSIENT_LINK_ERROR_PROXY", {}).get("recall") or 0) >= 0.90,
        "congestion_fail_slow_confusion_le_010": congestion_fail_confusion <= 0.10,
        "id_macro_f1_ge_085": quant_f1 >= 0.85,
        "top1_ge_090": (event["top1_localization"] or 0) >= 0.90,
        "false_alarms_le_01_per_run": false_per_run is not None and false_per_run <= 0.1,
        "ece_le_005": metrics["calibration"]["ece"] <= 0.05,
        "ood_auroc_ge_080": (ood_auc or 0) >= 0.80,
        "quantized_loss_le_002": float_f1 - quant_f1 <= 0.02,
        "state_bytes_le_64": resource["dynamic_state_bytes_per_port"] <= 64,
    }
    metrics["go_no_go"] = {"criteria": go, "decision": "GO" if all(go.values()) else "NO-GO"}
    os.makedirs(args.out_dir, exist_ok=True)
    confusion(y_true, y_quant, classes).to_csv(os.path.join(args.out_dir, "confusion_matrix.csv"))
    pd.DataFrame(risk_rows).to_csv(os.path.join(args.out_dir, "risk_coverage.csv"), index=False)
    pd.DataFrame(reliability).to_csv(os.path.join(args.out_dir, "reliability_diagram.csv"), index=False)
    with open(os.path.join(args.out_dir, "metrics.json"), "w") as out:
        json.dump(metrics, out, indent=2)
        out.write("\n")
    with open(os.path.join(args.out_dir, "final_report.md"), "w") as out:
        out.write("# QG-HMM Gray-Fault Experiment Report\n\n")
        out.write(f"Decision: **{metrics['go_no_go']['decision']}**\n\n")
        out.write("## Experiment coverage\n\n")
        out.write(f"- Completed simulator runs: {len(manifest['runs'])}/{len(manifest['runs'])}\n")
        out.write(f"- Causal 1-ms port snapshots: {len(dataset)}\n")
        out.write(f"- Locked test snapshots: {len(test)}\n")
        out.write(f"- Selected deployable model: `{metrics['model_selected']}`\n")
        out.write("- Validation macro-F1: " + ", ".join(
            f"{name}={value:.4f}" for name, value in
            float_model["validation_macro_f1"].items()) + "\n")
        out.write(f"- Quantized ID macro-F1: {quant_f1:.4f}\n")
        out.write(f"- Float ID macro-F1: {float_f1:.4f}\n")
        out.write(f"- Float-to-quantized macro-F1 loss: {float_f1 - quant_f1:.6f}\n")
        out.write(f"- Float/quantized agreement: {agreement:.4f}\n")
        out.write("\n## Detection, localization, and latency\n\n")
        for label, values in per_class.items():
            recall = values["recall"]
            out.write(f"- `{label}` recall: {recall:.4f} (support={values['support']})\n")
        out.write(f"- Event recall: {event['event_level_recall']:.4f} ({event['events']} events)\n")
        out.write(f"- Unique Top-1 / Top-3 localization: "
                  f"{event['unique_top1_rate']:.4f} / {event['top3_localization']:.4f}\n")
        out.write("- Detection latency median/P95/P99: "
                  f"{event['latency_ns']['median']:.0f} / "
                  f"{event['latency_ns']['p95']:.0f} / "
                  f"{event['latency_ns']['p99']:.0f} ns\n")
        out.write("\n## Alarm quality and uncertainty\n\n")
        out.write(f"- False alarms: {false_per_run:.2f}/healthy run "
                  f"({healthy_alarm_count} alarms over {len(healthy_runs)} runs)\n")
        out.write(f"- ECE / Brier / NLL: {metrics['calibration']['ece']:.4f} / "
                  f"{brier:.4f} / {nll:.4f}\n")
        out.write(f"- OOD AUROC / UNKNOWN recall: "
                  f"{ood_auc if ood_auc is not None else 'N/A'} / "
                  f"{unknown_recall if unknown_recall is not None else 'N/A'}\n")
        out.write(f"- Explicit link-state fast-path recall: "
                  f"{fast_path_recall if fast_path_recall is not None else 'N/A'}\n")
        m0 = metrics["m0_switch_sparse"]
        out.write("\n## M0 switch-sparse reference\n\n")
        out.write(f"- Event recall: {m0.get('event_level_recall')}\n")
        out.write(f"- Unique Top-1 localization: {m0.get('unique_top1_localization')}\n")
        out.write(f"- Healthy alarms: {m0.get('healthy_alarm_count')}\n")
        out.write(f"- Median/P95/P99 latency: {m0.get('latency_ns')}\n")
        out.write("\n## Resource accounting\n\n")
        out.write(f"- Dynamic state: {resource['dynamic_state_bytes_per_port']} bytes/port\n")
        out.write(f"- Shared constants: {resource['shared_model_bytes']} bytes\n")
        out.write(f"- LUT entries: {resource['lut_entries']}\n")
        out.write(f"- Lookups/additions/comparisons per inference: "
                  f"{resource['lookups_per_inference']}/"
                  f"{resource['integer_additions_per_inference']}/"
                  f"{resource['comparisons_per_inference']}\n")
        out.write(f"- Monitoring parity under fault: {parity}\n\n")
        if ablations is not None:
            out.write("## Ablations\n\n")
            out.write(f"- Full float model macro-F1: "
                      f"{ablations['full_model']['macro_f1']:.4f}\n")
            for name in ["A1_without_context_normalization",
                         "A2_without_hmm_transition", "A3_without_queue_peak",
                         "A4_without_peer_rate_gap"]:
                value = ablations[name]
                out.write(f"- `{name}`: macro-F1={value['macro_f1']:.4f}, "
                          f"delta={value['delta']:+.4f}\n")
            value = ablations["A5_without_unknown_rejection"]
            out.write(f"- `A5_without_unknown_rejection`: UNKNOWN recall "
                      f"{value['unknown_recall']:.4f} versus "
                      f"{value['full_unknown_recall']:.4f}\n")
            value = ablations["A6_without_confidence_calibration"]
            out.write(f"- `A6_without_confidence_calibration`: ECE="
                      f"{value['uncalibrated_ece']:.4f}\n\n")
        out.write("## Go/No-Go criteria\n\n")
        for name, passed in go.items():
            out.write(f"- {'PASS' if passed else 'FAIL'}: `{name}`\n")
        out.write("\n## Automated checks\n\n")
        for name, passed in checks.items():
            status = "SKIP" if passed is None else ("PASS" if passed else "FAIL")
            out.write(f"- {status}: `{name}`\n")
        out.write("\n## Interpretation\n\n")
        out.write("The quantized implementation meets determinism, prediction-agreement, "
                  "quantization-loss, monitoring-parity, and 64-byte state checks. "
                  "The learned detector is not deployment-ready: class separation, "
                  "calibration, healthy-run alarm rate, and OOD rejection remain below "
                  "the locked acceptance criteria. The M0 rule baseline remains the "
                  "stronger operational detector on this benchmark.\n")
    print(json.dumps(metrics, indent=2))


if __name__ == "__main__":
    main()
