#!/usr/bin/env python3
"""Run six real true-16 ns-3 split policies and emit B5 separately.

Output roots must be new. The default healthy calibration is measured from
the same workload on each rail, not invented capacity. Fault experiments must
provide an independently calibrated, time-varying oracle capacity schedule.
"""
import argparse
import csv
import hashlib
import json
import math
import os
from pathlib import Path
import resource
import subprocess
import sys

from split_capacity import read_capacity, future_bound
from simulator_runtime_bundle import seal_runtime_bundle, sealed_execution_environment, validate_runtime_bundle

ROOT = Path(__file__).resolve().parents[2]
POLICIES = ("B0", "B1", "B2", "B3", "B4", "B6", "B7", "B8", "B9", "B10", "B11", "B12", "B13", "B14")


def rows(path):
    with Path(path).open(newline="") as f:
        return list(csv.DictReader(f))


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def validate_static_faults(path, horizon):
    schedule = rows(path)
    if not schedule:
        raise ValueError("empty static fault schedule")
    links = set()
    for row in schedule:
        if (row["fault_type"] != "service_degradation" or int(row["start_time_ns"]) != 0
                or int(row["end_time_ns"]) < horizon or int(row["recovery_delay_ns"]) != 0
                or not 0 < float(row["parameter_after"]) <= 1):
            raise ValueError("static calibration requires a constant carrier-up service fraction for the whole horizon")
        if row["target_link_id"] in links:
            raise ValueError("overlapping static impairments")
        links.add(row["target_link_id"])


def audit(directory):
    events = rows(directory / "split_events.csv")
    pending, assigned, completed = {}, [0, 0], [0, 0]
    intervals, totals = {}, {}
    start, finish = None, 0
    for e in events:
        key = tuple(int(e[k]) for k in ("src", "dst", "sport"))
        rail, size, t = int(e["rail"]), int(e["bytes"]), int(e["timestamp_ns"])
        if rail not in (0, 1) or size <= 0:
            raise ValueError("invalid rail/payload")
        if e["event"] == "ASSIGN":
            if key in pending:
                raise ValueError("duplicate active chunk key")
            pending[key] = (rail, size, t)
            flow_key = key[:2]+(int(e["logical_flow_id"]),)
            total, offset = int(e["flow_bytes"]), int(e["chunk_offset"])
            if total <= 0 or offset < 0 or offset+size > total:
                raise ValueError("chunk outside logical flow")
            if flow_key in totals and totals[flow_key] != total:
                raise ValueError("inconsistent logical flow size")
            totals[flow_key] = total
            intervals.setdefault(flow_key, []).append((offset,offset+size))
            assigned[rail] += size
            start = t if start is None else min(start, t)
        elif e["event"] == "ACK_COMPLETE":
            if key not in pending:
                raise ValueError("ACK without live assignment")
            r, n, begun = pending.pop(key)
            if (r, n) != (rail, size) or t < begun:
                raise ValueError("ACK rail/byte/time mismatch")
            completed[rail] += size
            finish = max(finish, t)
        else:
            raise ValueError("unknown split event")
    lifecycle = rows(directory / "run_lifecycle.csv")
    for key, spans in intervals.items():
        cursor = 0
        for left, right in sorted(spans):
            if left != cursor:
                raise ValueError("duplicate, overlapping or missing logical chunk interval")
            cursor = right
        if cursor != totals[key]:
            raise ValueError("logical flow not fully assigned")
    done = [r for r in lifecycle if r["status"] == "WORKLOAD_COMPLETE"
            and int(r["finished_ranks"]) == 16 and int(r["world_size"]) == 16]
    passed = bool(events) and not pending and assigned == completed and len(done) == 1
    samples_path = directory / "split_samples.csv"
    informative = sum(int(r["informative"]) for r in rows(samples_path)) if samples_path.exists() else 0
    feedback = audit_feedback(directory, events) if (directory/"chunk_feedback.csv").exists() else None
    return {"pass": passed, "pending_chunks": len(pending), "chunk_feedback": feedback,
            "assigned_bytes": assigned, "acked_payload_bytes": completed,
            "transport_span_ns": finish-start if start is not None else None,
            "workload_finish_ns": int(done[0]["actual_ns"]) if done else None,
            "informative_samples": informative,
            "tensor_numeric_correctness": "not simulated"}


def audit_feedback(directory, events):
    assignments = {}
    for e in events:
        if e["event"] == "ASSIGN":
            assignments.setdefault(int(e["src"]), []).append(e)
    data = rows(directory/"chunk_feedback.csv")
    age_snapshots = audit_age_feedback(directory, data) if any(e['policy']=='B9' for e in data) else {}
    adaptive_mode = any(e['policy'] in ('B10','B11','B12') for e in data)
    censored_mode = any(e['policy'] in ('B13','B14') for e in data)
    first, counts = {}, {}
    for e in data:
        key = (int(e["src"]), int(e["rail"]))
        elapsed = int(e["completion_ns"])-int(e["first_tx_ns"])
        if elapsed <= 0 or elapsed != int(e["elapsed_ns"]):
            raise ValueError("invalid measured chunk duration")
        rate = int(e["bytes"])*8e9/elapsed
        if not math.isclose(rate, float(e["measured_bps"]), rel_tol=1e-5):
            raise ValueError("chunk rate not derived from actual first TX/ACK")
        counts[key] = counts.get(key,0)+1
        if int(e["rail_samples"]) != counts[key]:
            raise ValueError("chunk feedback sample counter mismatch")
        first.setdefault(key,e)
        expected = float(first[key]["measured_bps"]) if e["policy"]=="B7" else rate
        if e['policy']=='B9':
            snapshot=age_snapshots[(int(e['timestamp_ns']),int(e['src']),int(e['dst']),int(e['sport']),int(e['rail']))]
            expected=min(rate,float(snapshot['cap_bps'])) if int(snapshot['age_ns']) else rate
        if not adaptive_mode and not censored_mode and not math.isclose(expected,float(e["used_bps"]),rel_tol=1e-5):
            raise ValueError("fixed/continuous feedback policy mismatch")
    probes=[]
    for src, values in sorted(assignments.items()):
        if len(values)<2:
            raise ValueError("not enough chunks for paired probe qualification")
        a,b=values[:2]
        if ({int(a["rail"]),int(b["rail"])}!={0,1} or a["timestamp_ns"]!=b["timestamp_ns"]
                or a["bytes"]!=b["bytes"] or a["dst"]!=b["dst"]):
            raise ValueError("first probes must be equal-sized simultaneous distinct-rail chunks to same peer")
        fa,fb=first[src,0],first[src,1]
        if fa["first_tx_ns"]!=fb["first_tx_ns"]:
            raise ValueError("paired probes did not actually start transmitting simultaneously")
        probes.append({"src":src,"bytes":int(a["bytes"]),
                       "A_elapsed_ns":int(fa["elapsed_ns"]),"B_elapsed_ns":int(fb["elapsed_ns"]),
                       "A_weight":float(fb["elapsed_ns"])/(int(fa["elapsed_ns"])+int(fb["elapsed_ns"]))})
    if len(probes)!=16 or len(data)!=sum(e["event"]=="ACK_COMPLETE" for e in events):
        raise ValueError("feedback does not cover all 16 senders/all completed chunks")
    result = {"pass":True,"probes":probes,"completion_samples":len(data)}
    if adaptive_mode:
        from audit_adaptive_split import audit_adaptive
        result['adaptive'] = audit_adaptive(directory, data, events)
    if censored_mode:
        from audit_censored_split import audit_censored
        result['censored'] = audit_censored(directory, data, events)
    return result


def audit_age_feedback(directory, feedback):
    """Check ACK-only updates, raw estimates, censored bounds and real TX witnesses."""
    records=rows(directory/'chunk_age_feedback.csv')
    if len(records)!=2*len(feedback):
        raise ValueError('B9 requires exactly two rail snapshots per completion')
    witnesses={(int(e['src']),int(e['dst']),int(e['sport']),int(e['first_tx_ns'])):e for e in feedback}
    snapshots,latest={},{}
    for index,event in enumerate(feedback):
        now,src,dst,sport,trigger=(int(event[k]) for k in ('timestamp_ns','src','dst','sport','rail'))
        latest[src,trigger]=float(event['measured_bps'])
        for rail,r in enumerate(records[2*index:2*index+2]):
            if tuple(int(r[k]) for k in ('timestamp_ns','src','trigger_dst','trigger_sport','trigger_rail','rail'))!=(now,src,dst,sport,trigger,rail):
                raise ValueError('non-ACK-triggered or misordered B9 snapshot')
            raw=float(r['raw_bps']); used=float(r['used_bps']); age=int(r['age_ns'])
            if not math.isclose(raw,latest.get((src,rail),0),rel_tol=1e-5):
                raise ValueError('B9 raw estimate is not latest completion')
            expected=raw
            if age:
                first_tx=int(r['witness_first_tx_ns'])
                witness=witnesses.get((src,int(r['witness_dst']),int(r['witness_sport']),first_tx))
                if (not witness or first_tx>=now or now-first_tx!=age
                        or int(witness['completion_ns'])<now or int(witness['rail'])!=rail
                        or int(witness['bytes'])!=int(r['witness_bytes'])
                        or (int(r['witness_dst']),int(r['witness_sport']))==(dst,sport)):
                    raise ValueError('B9 witness not a real distinct in-flight chunk')
                cap=int(r['witness_bytes'])*8e9/age
                if not math.isclose(cap,float(r['cap_bps']),rel_tol=1e-5):
                    raise ValueError('invalid B9 censored bound')
                expected=min(raw,cap)
            if not math.isclose(used,expected,rel_tol=1e-5):
                raise ValueError('invalid B9 effective estimate')
            snapshots[now,src,dst,sport,rail]=r
    return snapshots


def calibration(paths, output):
    """Observed ACK payload / union of active chunk intervals, per pair/rail."""
    result = []
    for rail, path in enumerate(paths):
        groups, pending = {}, {}
        for e in rows(path / "split_events.csv"):
            key = tuple(int(e[k]) for k in ("src", "dst", "sport"))
            if int(e["rail"]) != rail:
                raise ValueError("calibration escaped its single rail")
            if e["event"] == "ASSIGN":
                pending[key] = (int(e["timestamp_ns"]), int(e["bytes"]))
            else:
                start, size = pending.pop(key)
                groups.setdefault(key[:2], []).append((start, int(e["timestamp_ns"]), size))
        if pending:
            raise ValueError("incomplete calibration")
        for (src, dst), spans in sorted(groups.items()):
            intervals = sorted((a, b) for a, b, _ in spans)
            left, right = intervals[0]
            duration = 0
            for a, b in intervals[1:]:
                if a > right:
                    duration += right-left
                    left, right = a, b
                else:
                    right = max(right, b)
            duration += right-left
            if duration <= 0:
                raise ValueError("zero calibration active time")
            result.append((src, dst, rail, 0, sum(s for _, _, s in spans)*8e9/duration))
    with output.open("w", newline="") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(("src", "dst", "rail", "start_ns", "capacity_bps"))
        writer.writerows(result)


def run_one(args, name, policy, capacity=None, rail=None):
    directory = args.out/name
    directory.mkdir()
    raw, log = directory/"raw", directory/"astra_log"
    raw.mkdir(); log.mkdir()
    # Avoid overwriting the shared output paths in the historical config.
    config_lines = []
    for line in args.config.read_text().splitlines():
        fields = line.split()
        if fields and (fields[0].endswith("_FILE") or fields[0].endswith("_OUTPUT_FILE")):
            line = f"{fields[0]} {raw / Path(fields[1]).name}"
        config_lines.append(line)
    config = directory/"SimAI.conf"
    config.write_text("\n".join(config_lines)+"\n")
    env = {k: v for k, v in os.environ.items() if not k.startswith(("LIMER_", "AS_"))}
    env.update(ASTRA_SIM_LOG_DIR=str(log), LIMER_TELEMETRY_ENABLE="1",
               LIMER_TELEMETRY_INTERVAL_US=str(args.sample_us),
               LIMER_TELEMETRY_DIR=str(directory), LIMER_RUN_ID=name,
               LIMER_SPLIT_POLICY=policy, LIMER_SPLIT_CHUNK_BYTES=str(args.chunk_bytes),
               LIMER_SPLIT_MAX_ACTIVE=str(args.max_active),
               LIMER_RDMA_RECOVERY_TRANSPORT_ENABLE="0",
               LIMER_OBSERVATION_STOP_NS=str(args.horizon_ns),
               AS_SEND_LAT="3", AS_NVLS_ENABLE="1", AS_PXN_ENABLE="0", AS_LOG_LEVEL="1")
    if capacity:
        env["LIMER_SPLIT_CAPACITY_CSV"] = str(capacity)
    if rail is not None:
        env["LIMER_SPLIT_SINGLE_RAIL"] = str(rail)
    if args.faults:
        env["LIMER_FAULT_SCHEDULE"] = str(args.faults)
    extra = getattr(args, 'extra_env', {})
    if any(not k.startswith(('LIMER_SPLIT_WAIT_', 'LIMER_SPLIT_SIGNAL_', 'LIMER_SPLIT_CENSORED_')) for k in extra):
        raise ValueError('extra_env only supports adaptive wait/signal/censored settings')
    env.update({k: str(v) for k, v in extra.items()})
    env, loader_audit = sealed_execution_environment(args.bundle, base_environment=env)
    command = [str(args.binary), "-t", str(args.threads), "-w", str(args.workload),
               "-n", str(args.topology), "-c", str(config)]
    manifest = {"command": command, "policy": policy, "rail_override": rail,
                "loader": loader_audit,
                "binary_sha256": digest(args.binary), "topology_sha256": digest(args.topology),
                "workload_sha256": digest(args.workload),
                "capacity_sha256": digest(capacity) if capacity else None,
                "fault_sha256": digest(args.faults) if args.faults else None,
                "env": {k: v for k, v in env.items() if k.startswith(("LIMER_", "AS_"))}}
    (directory/"manifest.json").write_text(json.dumps(manifest, indent=2)+"\n")
    with (directory/"run.log").open("w") as stream:
        try:
            result = subprocess.run(command, env=env, cwd=raw, stdout=stream,
                                    stderr=subprocess.STDOUT, timeout=args.timeout)
            code = result.returncode
        except subprocess.TimeoutExpired:
            code = 124
    (directory/"exit_code.txt").write_text(str(code)+"\n")
    validate_runtime_bundle(args.bundle.root)
    if code:
        return {"pass": False, "exit_code": code, "reason": "see run.log"}
    try:
        return audit(directory)
    except (ValueError, KeyError, OSError) as exc:
        return {"pass": False, "reason": str(exc)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--topology", type=Path, required=True)
    parser.add_argument("--workload", type=Path, default=ROOT/"limer/configs/microAllReduce_16rank_smoke.txt")
    parser.add_argument("--config", type=Path, default=ROOT/"limer/configs/SimAI.baseline.conf")
    parser.add_argument("--capacity", type=Path)
    parser.add_argument("--faults", type=Path)
    parser.add_argument("--calibrate-static-faults", action="store_true",
                        help="measure both rails under constant t=0 service degradation before comparison")
    parser.add_argument("--policies", nargs="+", choices=POLICIES, default=list(POLICIES[:6]))
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--chunk-bytes", type=int, default=65536)
    parser.add_argument("--max-active", type=int, default=8)
    parser.add_argument("--sample-us", type=int, default=1000)
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument("--timeout", type=int, default=180)
    parser.add_argument("--horizon-ns", type=int, default=100000000)
    args = parser.parse_args()
    for field in ("binary", "topology", "workload", "config", "capacity", "faults", "out"):
        if getattr(args, field) is not None:
            setattr(args, field, getattr(args, field).resolve())
    needs_capacity = any(p in ("B2","B3","B4") for p in args.policies)
    if len(set(args.policies)) != len(args.policies):
        parser.error("duplicate policy")
    if args.faults and needs_capacity and not args.capacity and not args.calibrate_static_faults:
        parser.error("fault comparisons require independently calibrated time-varying --capacity")
    if args.calibrate_static_faults:
        if not args.faults or args.capacity:
            parser.error("--calibrate-static-faults requires --faults and no --capacity")
        validate_static_faults(args.faults, args.horizon_ns)
    if args.threads != 1:
        parser.error("split baseline qualification currently requires one simulator worker; topology remains true-16")
    if min(args.chunk_bytes, args.max_active, args.sample_us, args.threads, args.timeout, args.horizon_ns) <= 0:
        parser.error("sizes, periods, concurrency and timeout must be positive")
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    args.out.mkdir(parents=True, exist_ok=False)
    subprocess.run([sys.executable, str(ROOT/"limer/tools/validate_true16_dualrail.py"),
                    "--topology", str(args.topology), "--out-json", str(args.out/"topology_validation.json")], check=True,
                   stdout=subprocess.DEVNULL)
    args.bundle = seal_runtime_bundle(args.out, args.binary)
    args.binary = args.bundle.executable
    report = {"results": {}, "scope": "true16_chunk_qp_split", "GP": False}
    if not args.capacity and needs_capacity:
        for rail in (0, 1):
            name = f"calibration_{rail}"
            report["results"][name] = run_one(args, name, "B0", rail=rail)
            if not report["results"][name]["pass"]:
                (args.out/"summary.json").write_text(json.dumps(report, indent=2)+"\n")
                return 1
        args.capacity = args.out/"capacity.csv"
        calibration([args.out/"calibration_0", args.out/"calibration_1"], args.capacity)
        report["calibration_scope"] = "workload-conditioned observed goodput; not guaranteed saturation"
    series = read_capacity(args.capacity) if args.capacity else None
    for policy in args.policies:
        report["results"][policy] = run_one(args, policy, policy, args.capacity if policy in ("B2","B3","B4") else None)
        print(policy, report["results"][policy], flush=True)
        (args.out/"summary.json").write_text(json.dumps(report, indent=2)+"\n")
    totals = {sum(r["acked_payload_bytes"]) for k,r in report["results"].items()
              if k in POLICIES and r.get("pass")}
    report["same_payload_across_policies"] = len(totals)==1
    payloads = {}
    for event in rows(args.out/"B0/split_events.csv") if (args.out/"B0/split_events.csv").exists() else []:
        if event["event"] == "ASSIGN":
            key = (int(event["src"]), int(event["dst"]))
            payloads[key] = payloads.get(key, 0)+int(event["bytes"])
    report["B5"] = {"fidelity": "conditional_independent_pair_fluid_bound",
                    "available": series is not None and bool(payloads),
                    "full_allreduce_bound": False,
                    "pairs": [{"src": s, "dst": d, "bytes": size,
                               **future_bound(series, s, d, size)}
                              for (s, d), size in sorted(payloads.items())] if series else []}
    (args.out/"summary.json").write_text(json.dumps(report, indent=2, allow_nan=False)+"\n")
    return 0 if report["same_payload_across_policies"] and all(v["pass"] for v in report["results"].values()) else 1


if __name__ == "__main__":
    sys.exit(main())
