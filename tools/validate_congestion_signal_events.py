#!/usr/bin/env python3
"""Fail-closed validation of live congestion-mechanism evidence."""

from __future__ import annotations
import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Sequence

SCHEMA_VERSION = "limer.p2-congestion-signal-events.v2"
UINT = re.compile(r"(?:0|[1-9][0-9]*)\Z")
IDENT = re.compile(r"[A-Za-z0-9_.:-]{1,128}\Z")
BOUND = "OBSERVED_REAL_MECHANISM_BACKGROUND_BOUND"
UNBOUND = "OBSERVED_REAL_MECHANISM_UNBOUND"
EVENTS = set(
    "ECN_MARK CNP_ACK_EMIT CNP_ACK_RX QP_RATE_DECREASE PFC_PAUSE_SEND PFC_PAUSE_RECEIVE PFC_RESUME_SEND PFC_RESUME_RECEIVE".split()
)
SCENARIOS = set("incast queue_buildup ecmp_collision ecn_pressure pfc_pressure".split())
TYPES = {"HOST", "SWITCH", "NVSWITCH"}
COLUMNS = tuple(
    "run_id event_id flow_id scenario event_type timestamp_ns node_id node_type port_id link_id trigger_out_port trigger_link_id pg flow_src_rank flow_dst_rank flow_sport flow_dport packet_sip packet_dip packet_sport packet_dport queue_bytes shared_used_bytes threshold_low_bytes threshold_high_bytes headroom_bytes signal_before signal_after pause_time old_rate_bps new_rate_bps cc_mode status".split()
)
LINK_COLUMNS = tuple(
    "link_id src_node dst_node src_type dst_type src_port dst_port link_class bandwidth_bps delay_ns".split()
)
SCHEDULE_COLUMNS = tuple(
    "event_id flow_id scenario scheduled_start_ns src_rank dst_rank bytes pg sport dport".split()
)
SUMMARY_COLUMNS = tuple(
    "run_id window_start_ns window_end_ns max_events seen recorded unbound filtered overflow status".split()
)


class SignalError(ValueError):
    def __init__(self, code: str, detail: str):
        super().__init__(detail)
        self.code = code


def _rows(path: Path, cols: tuple[str, ...], label: str, empty_ok=False):
    with path.open(newline="", encoding="utf-8") as f:
        rd = csv.DictReader(f)
        if tuple(rd.fieldnames or ()) != cols:
            raise SignalError("CSV_SCHEMA", f"{label} header is not exact")
        rows = list(rd)
    if not rows and not empty_ok:
        raise SignalError("CSV_NO_ROWS", f"{label} has no rows")
    return rows


def _u(v: str, name: str, maximum=2**64 - 1, optional=False):
    if optional and v == "":
        return None
    if not UINT.fullmatch(v):
        raise SignalError("INTEGER", f"{name} is not canonical unsigned")
    n = int(v)
    if n > maximum:
        raise SignalError("INTEGER_RANGE", f"{name} exceeds range")
    return n


def _id(v: str, name: str):
    if not IDENT.fullmatch(v):
        raise SignalError("IDENTIFIER", f"{name} is empty or invalid")


def _tuple(row, names, line):
    p = [row[n] != "" for n in names]
    if any(p) and not all(p):
        raise SignalError("TUPLE_PARTIAL", f"line {line} partial tuple")
    return all(p)


def validate(
    *,
    events: Path,
    link_map: Path,
    schedule: Path,
    summary: Path,
    expected_run_id: str,
    profile="RAW",
    gpus_per_server=8,
) -> dict[str, Any]:
    try:
        if profile not in {"RAW", "ECN", "PFC"} or gpus_per_server <= 0:
            raise SignalError("ARGUMENT", "bad profile/gpus_per_server")
        _id(expected_run_id, "expected_run_id")
        endpoints = {}
        link_ends = {}
        for line, r in enumerate(_rows(link_map, LINK_COLUMNS, "link_map"), 2):
            a = _u(r["src_node"], "src_node", 2**32 - 1)
            b = _u(r["dst_node"], "dst_node", 2**32 - 1)
            ap = _u(r["src_port"], "src_port", 2**32 - 1)
            bp = _u(r["dst_port"], "dst_port", 2**32 - 1)
            _u(r["bandwidth_bps"], "bandwidth_bps")
            _u(r["delay_ns"], "delay_ns")
            lid = f"L{min(a, b)}-{max(a, b)}"
            if (
                a == b
                or r["link_id"] != lid
                or r["src_type"] not in TYPES
                or r["dst_type"] not in TYPES
                or not r["link_class"]
            ):
                raise SignalError("LINK_ENDPOINT", f"link line {line} invalid")
            for key, typ, peer in (
                ((a, ap), r["src_type"], b),
                ((b, bp), r["dst_type"], a),
            ):
                if key in endpoints:
                    raise SignalError("LINK_ENDPOINT", "ambiguous node/port")
                endpoints[key] = (lid, typ, peer)
            link_ends[lid] = {a, b}
        declared = {}
        declared_event_ids = set()
        declared_flow_ids = set()
        for line, r in enumerate(_rows(schedule, SCHEDULE_COLUMNS, "schedule"), 2):
            _id(r["event_id"], "event_id")
            _id(r["flow_id"], "flow_id")
            if r["scenario"] not in SCENARIOS:
                raise SignalError("SCENARIO", f"schedule line {line}")
            start = _u(r["scheduled_start_ns"], "start")
            src = _u(r["src_rank"], "src", 2**32 - 1)
            dst = _u(r["dst_rank"], "dst", 2**32 - 1)
            size = _u(r["bytes"], "bytes")
            pg = _u(r["pg"], "pg", 7)
            sport = _u(r["sport"], "sport", 65535)
            dport = _u(r["dport"], "dport", 65535)
            key = (src, dst, sport, pg)
            if (
                src == dst
                or src // gpus_per_server == dst // gpus_per_server
                or not size
                or not pg
                or sport < 49152
                or not dport
                or key in declared
                or r["event_id"] in declared_event_ids
                or r["flow_id"] in declared_flow_ids
            ):
                raise SignalError("SCHEDULE", f"schedule line {line}")
            declared[key] = (r, start)
            declared_event_ids.add(r["event_id"])
            declared_flow_ids.add(r["flow_id"])
        metas = _rows(summary, SUMMARY_COLUMNS, "summary")
        if len(metas) != 1:
            raise SignalError("SUMMARY", "one summary row required")
        m = metas[0]
        if m["run_id"] != expected_run_id or m["status"] != "PASS":
            raise SignalError("SUMMARY", "run/status")
        ws = _u(m["window_start_ns"], "window_start")
        we = _u(m["window_end_ns"], "window_end")
        cap = _u(m["max_events"], "max_events")
        seen = _u(m["seen"], "seen")
        rec = _u(m["recorded"], "recorded")
        ub = _u(m["unbound"], "unbound")
        filt = _u(m["filtered"], "filtered")
        ov = _u(m["overflow"], "overflow")
        if (
            not cap
            or cap > 10_000_000
            or ws != min(start for _, start in declared.values())
            or ws > we
            or ov
            or seen != rec + filt + ov
            or rec > cap
        ):
            raise SignalError("SUMMARY_INVARIANT", "counter/window/overflow")
        rows = _rows(events, COLUMNS, "events", empty_ok=profile == "RAW")
        if rec != len(rows):
            raise SignalError("SUMMARY_INVARIANT", "recorded/raw mismatch")
        counts = Counter()
        chains = defaultdict(list)
        receives = []
        actual_ub = 0
        for line, r in enumerate(rows, 2):
            kind = r["event_type"]
            if r["run_id"] != expected_run_id or kind not in EVENTS:
                raise SignalError("EVENT_BINDING", f"line {line} run/type")
            t = _u(r["timestamp_ns"], "timestamp")
            node = _u(r["node_id"], "node", 2**32 - 1)
            port = _u(r["port_id"], "port", 2**32 - 1)
            pg = _u(r["pg"], "pg", 7)
            ep = endpoints.get((node, port))
            if (
                not pg
                or not ws <= t <= we
                or ep is None
                or ep[:2] != (r["link_id"], r["node_type"])
            ):
                raise SignalError("PHYSICAL_LINK", f"line {line}")
            trig = _u(r["trigger_out_port"], "trigger", 2**32 - 1, optional=True)
            if (
                (trig is None and r["trigger_link_id"])
                or (trig is not None and (node, trig) not in endpoints)
                or (
                    trig is not None
                    and endpoints[(node, trig)][0] != r["trigger_link_id"]
                )
            ):
                raise SignalError("TRIGGER_LINK", f"line {line}")
            fn = ("flow_src_rank", "flow_dst_rank", "flow_sport", "flow_dport")
            pn = ("packet_sip", "packet_dip", "packet_sport", "packet_dport")
            hf = _tuple(r, fn, line)
            hp = _tuple(r, pn, line)
            flow = (
                tuple(_u(r[n], n, 65535 if "port" in n else 2**32 - 1) for n in fn)
                if hf
                else None
            )
            if hp:
                for n in pn:
                    _u(r[n], n, 65535 if "port" in n else 2**32 - 1)
            q = _u(r["queue_bytes"], "queue", optional=True)
            sh = _u(r["shared_used_bytes"], "shared", optional=True)
            lo = _u(r["threshold_low_bytes"], "low", optional=True)
            hi = _u(r["threshold_high_bytes"], "high", optional=True)
            hr = _u(r["headroom_bytes"], "headroom", optional=True)
            if (lo is None) != (hi is None):
                raise SignalError("THRESHOLD_PARTIAL", f"line {line}")
            before = _u(r["signal_before"], "before", 3)
            after = _u(r["signal_after"], "after", 3)
            pause = _u(r["pause_time"], "pause", 65535)
            old = _u(r["old_rate_bps"], "old", optional=True)
            new = _u(r["new_rate_bps"], "new", optional=True)
            _u(r["cc_mode"], "cc", 2**32 - 1)
            if (old is None) != (new is None):
                raise SignalError("RATE_PARTIAL", f"line {line}")
            if r["status"] == BOUND:
                _id(r["event_id"], "event_id")
                _id(r["flow_id"], "flow_id")
                d = declared.get((flow[0], flow[1], flow[2], pg)) if flow else None
                if (
                    r["scenario"] not in SCENARIOS
                    or d is None
                    or d[0]["dport"] != str(flow[3])
                    or any(r[x] != d[0][x] for x in ("event_id", "flow_id", "scenario"))
                    or t < d[1]
                ):
                    raise SignalError("EVENT_BINDING", f"line {line} schedule")
                chains[r["flow_id"]].append((t, kind, r))
            elif r["status"] == UNBOUND:
                actual_ub += 1
                if any(r[x] for x in ("event_id", "flow_id", "scenario")):
                    raise SignalError("EVENT_BINDING", f"line {line} unbound labels")
            else:
                raise SignalError("STATUS", f"line {line}")
            blank = sh is None and lo is None and hi is None and hr is None
            data_ports = (
                hf
                and hp
                and (r["packet_sport"], r["packet_dport"])
                == (r["flow_sport"], r["flow_dport"])
            )
            ack_ports = (
                hf
                and hp
                and (r["packet_sport"], r["packet_dport"])
                == (r["flow_dport"], r["flow_sport"])
            )
            if kind == "ECN_MARK":
                ok = (
                    r["status"] == BOUND
                    and r["node_type"] != "HOST"
                    and hf
                    and hp
                    and data_ports
                    and q is not None
                    and hi is not None
                    and sh is None
                    and hr is None
                    and after == 3
                    and not pause
                    and old is None
                    and trig is None
                )
            elif kind in {"CNP_ACK_EMIT", "CNP_ACK_RX"}:
                signal_ok = after == 1 and (
                    before in {1, 2, 3} if kind == "CNP_ACK_EMIT" else before == 0
                )
                ok = (
                    r["status"] == BOUND
                    and hf
                    and hp
                    and ack_ports
                    and q is None
                    and blank
                    and signal_ok
                    and not pause
                    and old is None
                    and trig is None
                )
            elif kind == "QP_RATE_DECREASE":
                ok = (
                    r["status"] == BOUND
                    and r["node_type"] == "HOST"
                    and hf
                    and not hp
                    and q is None
                    and blank
                    and old is not None
                    and 0 < new < old
                    and (before, after) == (0, 0)
                    and not pause
                    and trig is None
                )
            elif kind.endswith("_SEND"):
                ok = (
                    r["status"] == BOUND
                    and r["node_type"] != "HOST"
                    and hf
                    and hp
                    and data_ports
                    and trig is not None
                    and q is not None
                    and None not in (sh, lo, hi, hr)
                    and (before, after) == ((0, 1) if "PAUSE" in kind else (1, 0))
                    and old is None
                    and (pause > 0 if "PAUSE" in kind else pause == 0)
                )
            else:
                ok = (
                    r["status"] == UNBOUND
                    and not hf
                    and not hp
                    and trig is None
                    and q is not None
                    and blank
                    and (before, after) == ((0, 1) if "PAUSE" in kind else (1, 0))
                    and old is None
                    and (pause > 0 if "PAUSE" in kind else pause == 0)
                )
                receives.append((t, kind, pg, node, r["link_id"]))
            if not ok:
                raise SignalError(kind + "_CONTRACT", f"line {line}")
            counts[kind] += 1
        if actual_ub != ub:
            raise SignalError("SUMMARY_INVARIANT", "unbound/raw mismatch")
        if profile == "ECN":
            if not any(_ecn_chain(c) for c in chains.values()):
                raise SignalError("ECN_CHAIN", "missing bound chain")
        if profile == "PFC" and not _pfc_chain(chains, receives, link_ends):
            raise SignalError("PFC_CHAIN", "missing physical chain")
        return {
            "schema_version": SCHEMA_VERSION,
            "status": "PASS",
            "profile": profile,
            "event_counts": dict(sorted(counts.items())),
            "errors": [],
        }
    except (OSError, csv.Error, SignalError) as e:
        return {
            "schema_version": SCHEMA_VERSION,
            "status": "FAIL",
            "profile": profile,
            "event_counts": {},
            "errors": [
                {
                    "code": e.code if isinstance(e, SignalError) else "ARTIFACT_READ",
                    "detail": str(e),
                }
            ],
        }


def _ecn_chain(chain):
    required = ["ECN_MARK", "CNP_ACK_EMIT", "CNP_ACK_RX", "QP_RATE_DECREASE"]
    selected = []
    for item in sorted(chain):
        if len(selected) < len(required) and item[1] == required[len(selected)]:
            selected.append(item)
    if len(selected) != len(required):
        return False
    mark, emit, received, _ = (item[2] for item in selected)
    fields = ("packet_sip", "packet_dip", "packet_sport", "packet_dport")
    data = tuple(mark[name] for name in fields)
    ack = tuple(emit[name] for name in fields)
    wire_rx = tuple(received[name] for name in fields)
    return ack == (data[1], data[0], data[3], data[2]) and wire_rx == ack


def _pfc_chain(chains, receives, link_ends):
    for c in chains.values():
        for pt, pk, p in sorted(c):
            if pk != "PFC_PAUSE_SEND":
                continue
            lid = p["link_id"]
            pg = int(p["pg"])
            sender = int(p["node_id"])
            for rt, rk, rpg, rnode, rlink in receives:
                if (
                    rk != "PFC_PAUSE_RECEIVE"
                    or rt < pt
                    or rpg != pg
                    or rlink != lid
                    or rnode == sender
                    or rnode not in link_ends[lid]
                ):
                    continue
                for st, sk, s in sorted(c):
                    if (
                        sk == "PFC_RESUME_SEND"
                        and st >= rt
                        and s["link_id"] == lid
                        and int(s["pg"]) == pg
                        and any(
                            k == "PFC_RESUME_RECEIVE"
                            and t >= st
                            and g == pg
                            and n == rnode
                            and recv_link == lid
                            for t, k, g, n, recv_link in receives
                        )
                    ):
                        return True
    return False


def main(argv: Sequence[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    for x in ("events", "link-map", "schedule", "summary"):
        p.add_argument("--" + x, required=True, type=Path)
    p.add_argument("--expected-run-id", required=True)
    p.add_argument("--profile", choices=("RAW", "ECN", "PFC"), default="RAW")
    p.add_argument("--gpus-per-server", required=True, type=int)
    p.add_argument("--output", type=Path)
    a = p.parse_args(argv)
    r = validate(
        events=a.events,
        link_map=a.link_map,
        schedule=a.schedule,
        summary=a.summary,
        expected_run_id=a.expected_run_id,
        profile=a.profile,
        gpus_per_server=a.gpus_per_server,
    )
    text = json.dumps(r, sort_keys=True, indent=2) + "\n"
    a.output.write_text(text, encoding="utf-8") if a.output else print(text, end="")
    return 0 if r["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
