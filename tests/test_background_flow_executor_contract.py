#!/usr/bin/env python3
"""Static contracts for the minimal real RDMA background-flow executor.

These tests guard wiring and provenance boundaries without claiming an ns-3
runtime result.  The authoritative build and true-16 incast/queue runs remain
separate stage-gate evidence.
"""

from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
FRONTEND = ROOT / "astra-sim-alibabacloud/astra-sim/network_frontend/ns3"
NS3 = ROOT / "ns-3-alibabacloud/simulation/src"


def function_body(source: str, signature: str) -> str:
    start = source.index(signature)
    brace = source.index("{", start)
    depth = 0
    for index in range(brace, len(source)):
        if source[index] == "{":
            depth += 1
        elif source[index] == "}":
            depth -= 1
            if depth == 0:
                return source[brace : index + 1]
    raise AssertionError(f"unterminated function: {signature}")


class BackgroundScheduleContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.executor = (FRONTEND / "limer_background_flow.h").read_text(
            encoding="utf-8"
        )
        cls.entry = (FRONTEND / "entry.h").read_text(encoding="utf-8")
        cls.main = (FRONTEND / "AstraSimNetwork.cc").read_text(
            encoding="utf-8"
        )
        cls.telemetry = (FRONTEND / "limer_telemetry.h").read_text(
            encoding="utf-8"
        )
        cls.common = (FRONTEND / "common.h").read_text(encoding="utf-8")

    def test_schedule_is_independent_strict_and_fail_closed(self) -> None:
        self.assertIn('std::getenv("LIMER_BACKGROUND_FLOW_SCHEDULE")', self.executor)
        self.assertIn(
            '"event_id,flow_id,scenario,scheduled_start_ns,src_rank,dst_rank,"',
            self.executor,
        )
        self.assertIn("if (!ValidScenario(flow.scenario))", self.executor)
        for scenario in (
            "incast",
            "queue_buildup",
            "ecmp_collision",
            "ecn_pressure",
            "pfc_pressure",
        ):
            self.assertIn(f'value == "{scenario}"', self.executor)
        self.assertIn("expected exactly 10 CSV fields", self.executor)
        self.assertIn("duplicate (src,dst,sport,pg) RDMA QP identity", self.executor)
        self.assertIn("std::exit(2)", self.executor)
        self.assertIn("schedule contains no background flow rows", self.executor)

    def test_real_clients_are_preinstalled_at_absolute_schedule_time(self) -> None:
        install = function_body(self.executor, "void Install(const FlowSpec& flow)")
        self.assertIn("ns3::RdmaClientHelper", install)
        self.assertIn('"LimerTrafficClass"', install)
        self.assertIn("LIMER_RDMA_TRAFFIC_BACKGROUND", install)
        self.assertIn("helper.Install(n.Get(flow.src_rank))", install)
        self.assertIn("app.Start(ns3::NanoSeconds(flow.scheduled_start_ns))", install)
        self.assertNotIn("SendFlow(", install)

        main1 = function_body(self.entry, "int main1(")
        self.assertLess(
            main1.index("BackgroundFlowExecutor::Get().Init"),
            main1.index('std::cout << "Running Simulation.'),
        )

    def test_reserved_source_ports_do_not_alias_training(self) -> None:
        self.assertIn("LIMER_BACKGROUND_SPORT_MIN", self.executor)
        self.assertIn("[49152,65535]", self.executor)
        send_flow = function_body(self.entry, "void SendFlow(")
        self.assertIn("allocate_training_source_port", send_flow)
        allocator = function_body(
            self.entry, "static uint16_t allocate_training_source_port("
        )
        self.assertIn("LIMER_TRAINING_SPORT_MIN", self.entry)
        self.assertIn("LIMER_BACKGROUND_SPORT_MIN", self.entry)
        self.assertIn("training_source_ports.Reserve", allocator)
        self.assertLess(
            send_flow.index("allocate_training_source_port"),
            send_flow.index("RdmaClientHelper clientHelper"),
        )

    def test_sidecar_has_declared_start_and_ack_complete_lifecycle(self) -> None:
        self.assertIn("background_flow_application.csv", self.executor)
        self.assertIn('"SCHEDULED"', self.executor)
        self.assertIn('"START"', self.executor)
        self.assertIn('"COMPLETE"', self.executor)
        self.assertIn('"ACK_COMPLETE"', self.executor)
        self.assertIn('"CENSORED"', self.executor)
        self.assertIn('"STARTED_NOT_ACK_COMPLETE_AT_STOP"', self.executor)
        self.assertIn('"NOT_STARTED_AT_STOP"', self.executor)
        self.assertIn("m_firstTxTimestampNs", self.executor)
        self.assertIn("m_firstAckProgressTimestampNs", self.executor)
        self.assertIn("BackgroundFlowExecutor::Get().Close()", self.main)

    def test_runtime_callbacks_are_mtp_safe_and_do_not_write_files(self) -> None:
        started = function_body(self.executor, "void RecordStarted(")
        complete = function_body(self.executor, "void RecordComplete(")
        for body in (started, complete):
            self.assertIn("std::lock_guard<std::mutex>", body)
            self.assertNotIn("std::ofstream", body)
        self.assertIn("std::mutex mu_", self.executor)

    def test_close_emits_explicit_terminal_evidence_for_incomplete_flows(self) -> None:
        close = function_body(self.executor, "void Close()")
        self.assertIn("Simulator::Now().GetNanoSeconds()", close)
        self.assertIn("if (!flow.completed)", close)
        self.assertIn('AppendEvent(flow, "CENSORED"', close)

    def test_rdma_events_label_background_qps(self) -> None:
        self.assertIn("transport_epoch,\"\n        \"traffic_class,event", self.telemetry)
        on_rdma = function_body(self.telemetry, "void OnRdmaEvent(")
        self.assertIn("GetLimerTrafficClass()", on_rdma)
        self.assertIn('"BACKGROUND" : "TRAINING"', on_rdma)

    def test_rdma_events_record_the_effective_retry_and_rto_contract(self) -> None:
        self.assertIn(
            '"snd_una,snd_nxt,retry_count,retry_limit,rto_us\\n"',
            self.telemetry,
        )
        on_rdma = function_body(self.telemetry, "void OnRdmaEvent(")
        self.assertIn("qp ? U(qp->m_retryCount) : Empty()", on_rdma)
        self.assertIn("qp ? U(qp->m_retryLimit) : Empty()", on_rdma)
        self.assertIn("qp ? U(qp->m_rtoUs) : Empty()", on_rdma)

    def test_host_route_buckets_have_stable_physical_rail_meaning(self) -> None:
        self.assertIn("#include <algorithm>", self.common)
        routes = function_body(self.common, "void SetRoutingEntries()")
        self.assertIn("Give every host, switch, and NVSwitch", routes)
        self.assertIn("std::sort(nexts.begin(), nexts.end()", routes)
        self.assertIn("nbr2if[node][left].idx", routes)
        self.assertIn("nbr2if[node][right].idx", routes)
        self.assertIn("return left->GetId() < right->GetId()", routes)
        self.assertLess(
            routes.index("std::sort(nexts.begin(), nexts.end()"),
            routes.index("for (int k = 0; k < (int)nexts.size(); k++)"),
        )

    def test_link_map_and_collective_identity_are_canonical_and_explicit(self) -> None:
        build_map = function_body(self.telemetry, "void BuildLinkMap()")
        self.assertIn("std::sort(links_.begin(), links_.end()", build_map)
        self.assertIn("lhs.src_node < rhs.src_node", build_map)
        self.assertIn("lhs.src_port > rhs.src_port", build_map)
        self.assertIn(
            '"run_id,timestamp_ns,collective_seq,attempt,layer_num,"',
            self.telemetry,
        )
        self.assertIn('"message_size_bytes,event,rank_id,world_size,ready_ranks,"',
                      self.telemetry)
        append = function_body(self.telemetry, "void AppendCollectiveTxAt(")
        self.assertIn("collective_states_.find(sequence)", append)
        self.assertIn("state->second.layer_num", append)
        self.assertIn("state->second.message_size", append)
        self.assertIn("run_id_, U(timestamp_ns), U(sequence)", append)


class TrafficOwnershipContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.qp_h = (
            NS3 / "point-to-point/model/rdma-queue-pair.h"
        ).read_text(encoding="utf-8")
        cls.qp_cc = (
            NS3 / "point-to-point/model/rdma-queue-pair.cc"
        ).read_text(encoding="utf-8")
        cls.hw_h = (NS3 / "point-to-point/model/rdma-hw.h").read_text(
            encoding="utf-8"
        )
        cls.hw_cc = (NS3 / "point-to-point/model/rdma-hw.cc").read_text(
            encoding="utf-8"
        )
        cls.driver_h = (NS3 / "point-to-point/model/rdma-driver.h").read_text(
            encoding="utf-8"
        )
        cls.driver_cc = (NS3 / "point-to-point/model/rdma-driver.cc").read_text(
            encoding="utf-8"
        )
        cls.client = (NS3 / "applications/model/rdma-client.cc").read_text(
            encoding="utf-8"
        )
        cls.helper = (
            NS3 / "applications/helper/rdma-client-helper.cc"
        ).read_text(encoding="utf-8")
        cls.entry = (FRONTEND / "entry.h").read_text(encoding="utf-8")

    def test_traffic_class_defaults_training_and_propagates_to_qp(self) -> None:
        self.assertIn("LIMER_RDMA_TRAFFIC_TRAINING = 0", self.qp_h)
        self.assertIn("LIMER_RDMA_TRAFFIC_BACKGROUND = 1", self.qp_h)
        self.assertIn(
            "m_limerTrafficClass = LIMER_RDMA_TRAFFIC_TRAINING", self.qp_cc
        )
        self.assertIn("uint32_t limerTrafficClass", self.hw_h)
        add_qp = function_body(self.hw_cc, "void RdmaHw::AddQueuePair(")
        self.assertIn("qp->SetLimerTrafficClass(limerTrafficClass)", add_qp)
        self.assertIn("uint32_t limerTrafficClass", self.driver_h)
        driver_add = function_body(self.driver_cc, "void RdmaDriver::AddQueuePair(")
        self.assertIn("notifyAppSent, limerTrafficClass", driver_add)
        start = function_body(self.client, "void RdmaClient::StartApplication(")
        self.assertIn("m_limerTrafficClass", start)
        self.assertIn("limer_background_flow_started", start)
        self.assertLess(
            start.index("rdma->AddQueuePair"),
            start.index("limer_background_flow_started("),
        )

    def test_background_callbacks_bypass_all_astra_collective_maps(self) -> None:
        send_finish = function_body(self.entry, "void send_finish(")
        qp_finish = function_body(self.entry, "void qp_finish(")
        send_guard = send_finish.index("LIMER_RDMA_TRAFFIC_BACKGROUND")
        qp_guard = qp_finish.index("LIMER_RDMA_TRAFFIC_BACKGROUND")
        self.assertLess(send_guard, send_finish.index("sender_src_port_map"))
        self.assertLess(qp_guard, qp_finish.index("sender_src_port_map"))
        self.assertLess(qp_guard, qp_finish.index("pairRtt"))
        self.assertIn("BackgroundFlowExecutor::Get().RecordComplete(q)", qp_finish)
        background_prefix = qp_finish[: qp_finish.index("return;", qp_guard)]
        self.assertNotIn("RecordCollective", background_prefix)
        self.assertNotIn("waiting_to_", background_prefix)

    def test_background_rx_qp_cleanup_is_mtp_serialized(self) -> None:
        qp_finish = function_body(self.entry, "void qp_finish(")
        guard = qp_finish.index("LIMER_RDMA_TRAFFIC_BACKGROUND")
        end = qp_finish.index("return;", guard)
        background = qp_finish[guard:end]
        self.assertIn("MtpInterface::explicitCriticalSection", background)
        self.assertLess(
            background.index("MtpInterface::explicitCriticalSection"),
            background.index("rdma->m_rdma->DeleteRxQp"),
        )
        self.assertLess(
            background.index("rdma->m_rdma->DeleteRxQp"),
            background.index("RecordComplete"),
        )

    def test_completion_is_reached_from_ack_progress(self) -> None:
        receive_ack = function_body(self.hw_cc, "int RdmaHw::ReceiveAck(")
        self.assertIn("qp->Acknowledge", receive_ack)
        self.assertIn("if (qp->IsFinished())", receive_ack)
        self.assertIn("QpComplete(qp)", receive_ack)
        qp_complete = function_body(self.hw_cc, "void RdmaHw::QpComplete(")
        self.assertIn("LIMER_RDMA_WC_SUCCESS", qp_complete)
        self.assertIn("m_qpCompleteCallback(qp)", qp_complete)
        pkt_sent = function_body(self.hw_cc, "void RdmaHw::PktSent(")
        self.assertIn("m_hasFirstTxTimestamp", pkt_sent)
        self.assertIn("m_firstTxTimestampNs = now", pkt_sent)

    def test_training_baseline_keeps_hash_rail_and_has_no_implicit_rto(self) -> None:
        constructor = function_body(self.hw_cc, "RdmaHw::RdmaHw()")
        self.assertIn(
            'std::getenv("LIMER_RDMA_RECOVERY_TRANSPORT_ENABLE")',
            constructor,
        )
        self.assertIn("m_limerRecoveryTransportEnabled = false", constructor)
        self.assertIn(
            'transportEnv != NULL && std::string(transportEnv) == "1"',
            constructor,
        )

        add_qp = function_body(self.hw_cc, "void RdmaHw::AddQueuePair(")
        self.assertIn("uint32_t nic_idx = GetNicIdxOfQp(qp)", add_qp)
        self.assertIn("qp->m_primaryNicIdx = nic_idx", add_qp)
        self.assertIn("qp->m_activeNicIdx = nic_idx", add_qp)
        self.assertIn(
            "if (interServer && m_limerRecoveryTransportEnabled)", add_qp
        )
        self.assertLess(
            add_qp.index("uint32_t nic_idx = GetNicIdxOfQp(qp)"),
            add_qp.index("const std::vector<int> &candidates"),
        )

        arm_rto = function_body(self.hw_cc, "void RdmaHw::ArmRto(")
        expired = function_body(self.hw_cc, "void RdmaHw::RtoExpired(")
        for body in (arm_rto, expired):
            self.assertIn("!m_limerRecoveryTransportEnabled", body)
            self.assertIn("LIMER_RDMA_TRAFFIC_BACKGROUND", body)
            self.assertLess(
                body.index("!m_limerRecoveryTransportEnabled"),
                body.index("Simulator::Schedule", body.index("!m_limerRecoveryTransportEnabled")),
            )

    def test_backup_activation_requires_explicit_transport_opt_in(self) -> None:
        activate = function_body(self.hw_cc, "bool RdmaHw::ActivateBackup(")
        self.assertIn("if (!m_limerRecoveryTransportEnabled)", activate)
        self.assertLess(
            activate.index("if (!m_limerRecoveryTransportEnabled)"),
            activate.index("RecordWc"),
        )

    def test_callback_parameter_shadowing_is_fixed(self) -> None:
        set_fn = function_body(self.client, "void RdmaClient::SetFn(")
        self.assertIn("this->msg_handler = msg_handler", set_fn)
        self.assertIn("this->fun_arg = fun_arg", set_fn)
        self.assertNotIn("msg_handler = msg_handler;", set_fn.replace(
            "this->msg_handler = msg_handler;", ""
        ))
        self.assertIn(": msg_handler(msg_handler), fun_arg(fun_arg)", self.helper)


if __name__ == "__main__":
    unittest.main()
