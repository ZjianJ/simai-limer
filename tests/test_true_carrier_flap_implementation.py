#!/usr/bin/env python3
"""Static contract tests for the reversible, true-carrier flap path.

These tests deliberately do not claim runtime validation.  They guard the
semantic boundaries most likely to regress while the parent task performs the
single authoritative ns-3 build and true-16 micro-runs.
"""

from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
NS3 = ROOT / "ns-3-alibabacloud/simulation/src/point-to-point/model"
NETWORK = ROOT / "ns-3-alibabacloud/simulation/src/network/model"
MTP = ROOT / "ns-3-alibabacloud/simulation/src/mtp/model"
MPI = ROOT / "ns-3-alibabacloud/simulation/src/mpi/model"
COMMON = ROOT / "astra-sim-alibabacloud/astra-sim/network_frontend/ns3/common.h"
TELEMETRY = (
    ROOT
    / "astra-sim-alibabacloud/astra-sim/network_frontend/ns3/limer_telemetry.h"
)


def function_body(source: str, signature: str) -> str:
    """Extract one C/C++ function body using balanced braces."""
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


class TrueCarrierDeviceContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.header = (NS3 / "qbb-net-device.h").read_text(encoding="utf-8")
        cls.source = (NS3 / "qbb-net-device.cc").read_text(encoding="utf-8")

    def test_bring_up_restores_real_state_and_schedules_node_restart(self) -> None:
        self.assertIn("void BringUp();", self.header)
        body = function_body(self.source, "void QbbNetDevice::BringUp()")
        self.assertIn("m_paused[i] = false", body)
        self.assertIn("SetLimerForcedLinkDown(false)", body)
        self.assertIn("NotifyLinkUp()", body)
        self.assertIn("Simulator::ScheduleWithContext", body)
        self.assertIn("m_node->GetId()", body)
        self.assertIn("&QbbNetDevice::DequeueAndTransmit", body)
        self.assertIn("&QbbNetDevice::SwitchAsHostSend", body)
        self.assertIn("&QbbNetDevice::SwitchDequeueAndTransmit", body)
        self.assertNotIn("Simulator::ScheduleNow", body)
        self.assertNotIn("RecomputeRoutes", body)
        self.assertNotIn("RedistributeQp", body)
        self.assertNotIn("m_activeNicIdx", body)

    def test_epoch_drop_is_counted_as_receive_drop(self) -> None:
        body = function_body(
            self.source, "void QbbNetDevice::ReceiveLimerCarrierDrop"
        )
        self.assertIn("m_limerRxDrops++", body)
        self.assertIn("m_limerRxDropBytes += packet->GetSize()", body)
        receive = function_body(self.source, "QbbNetDevice::Receive(Ptr<Packet>")
        self.assertIn("ReceiveLimerCarrierDrop(packet)", receive)

    def test_take_down_accounts_host_ack_and_switch_queue_purges_as_drops(self) -> None:
        body = function_body(self.source, "void QbbNetDevice::TakeDown()")
        self.assertIn("m_rdmaEQ->m_ackQ->Dequeue()", body)
        self.assertGreaterEqual(body.count("m_limerTxDrops++"), 2)
        self.assertGreaterEqual(body.count("m_limerTxDropBytes += p->GetSize()"), 2)
        self.assertGreaterEqual(body.count("m_phyTxDropTrace(p)"), 2)
        self.assertIn("SwitchNotifyDrop", body)
        self.assertNotIn("SwitchNotifyDequeue", body)


class TrueCarrierChannelContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.header = (NS3 / "qbb-channel.h").read_text(encoding="utf-8")
        cls.source = (NS3 / "qbb-channel.cc").read_text(encoding="utf-8")

    def test_channel_uses_atomic_carrier_epoch(self) -> None:
        self.assertIn("std::atomic<bool> m_carrierUp", self.header)
        self.assertIn("std::atomic<uint64_t> m_carrierEpoch", self.header)
        down = function_body(self.source, "QbbChannel::NotifyCarrierDown")
        self.assertLess(down.index("exchange (false"), down.index("fetch_add"))

    def test_delivery_rejects_a_pre_down_epoch_even_after_up(self) -> None:
        transmit = function_body(self.source, "QbbChannel::TransmitStart")
        deliver = function_body(
            self.source, "QbbChannel::DeliverWithCarrierEpoch (Ptr<Packet>"
        )
        self.assertGreaterEqual(transmit.count("m_carrierEpoch.load"), 2)
        self.assertIn("&QbbChannel::DeliverWithCarrierEpoch", transmit)
        self.assertIn("carrierEpoch != m_carrierEpoch.load", deliver)
        self.assertIn("ReceiveLimerCarrierDrop", deliver)


class PhysicalLinkBoundaryTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.common = COMMON.read_text(encoding="utf-8")

    def test_down_and_up_update_both_nbr2if_directions(self) -> None:
        down = function_body(self.common, "bool PhysicalLinkDown")
        up = function_body(self.common, "bool PhysicalLinkUp")
        self.assertIn("a_to_b.up = b_to_a.up = false", down)
        self.assertIn("a_to_b.up = b_to_a.up = true", up)
        self.assertIn("a_dev->TakeDown()", down)
        self.assertIn("b_dev->TakeDown()", down)
        self.assertIn("a_dev->BringUp()", up)
        self.assertIn("b_dev->BringUp()", up)

    def test_physical_up_does_not_repair_fib_or_fail_back_qps(self) -> None:
        up = function_body(self.common, "bool PhysicalLinkUp")
        self.assertNotIn("RecomputeRoutesAfterLinkStateChange", up)
        self.assertNotIn("RedistributeQp", up)
        self.assertNotIn("Failover", up)
        self.assertLess(up.index("a_dev->BringUp()"), up.index("NotifyCarrierUp"))


class SwitchPurgeAccountingTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.node_header = (NETWORK / "node.h").read_text(encoding="utf-8")
        cls.switch = (NS3 / "switch-node.cc").read_text(encoding="utf-8")
        cls.nvswitch = (NS3 / "nvswitch-node.cc").read_text(encoding="utf-8")

    def test_purge_has_a_dedicated_virtual_boundary(self) -> None:
        self.assertIn("virtual void SwitchNotifyDrop", self.node_header)

    def test_switch_purge_releases_admission_without_tx_or_ecn_updates(self) -> None:
        body = function_body(self.switch, "void SwitchNode::SwitchNotifyDrop")
        self.assertIn("RemoveFromIngressAdmission", body)
        self.assertIn("RemoveFromEgressAdmission", body)
        self.assertIn("CheckAndSendResume", body)
        self.assertIn("Simulator::ScheduleWithContext", body)
        self.assertIn("GetId()", body)
        for forbidden in (
            "m_txBytes",
            "m_limerTxPkts",
            "m_limerEcnMarks",
            "ShouldSendCN",
            "PushHop",
            "m_lastPktTs",
        ):
            self.assertNotIn(forbidden, body)

    def test_nvswitch_purge_releases_admission_without_tx_updates(self) -> None:
        body = function_body(self.nvswitch, "void NVSwitchNode::SwitchNotifyDrop")
        self.assertIn("RemoveFromIngressAdmission", body)
        self.assertIn("RemoveFromEgressAdmission", body)
        self.assertNotIn("m_txBytes", body)
        self.assertNotIn("m_lastPktTs", body)


class FaultInjectorBoundaryTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.telemetry = TELEMETRY.read_text(encoding="utf-8")

    def test_true_and_legacy_flap_names_remain_distinct(self) -> None:
        supported = function_body(
            self.telemetry, "static bool IsSupportedFaultType"
        )
        self.assertIn('fault_type == "carrier_flap"', supported)
        self.assertIn('fault_type == "link_flap"', supported)
        self.assertIn("legacy_forced_state_rate_proxy", self.telemetry)

    def test_true_flap_has_explicit_down_and_up_sidecars(self) -> None:
        self.assertIn("PhysicalLinkDown(n.Get(src), n.Get(dst))", self.telemetry)
        self.assertIn("PhysicalLinkUp(n.Get(src), n.Get(dst))", self.telemetry)
        self.assertIn("physical_carrier_down_channel_epoch", self.telemetry)
        self.assertIn("physical_carrier_up_no_fib_or_qp_change", self.telemetry)

    def test_carrier_faults_use_per_link_owners_and_validate_intervals(self) -> None:
        self.assertIn("active_carrier_owners_", self.telemetry)
        self.assertIn("owners.insert(e.fault_id)", self.telemetry)
        self.assertIn("owners_it->second.erase(e.fault_id)", self.telemetry)
        self.assertIn("if (!owners_it->second.empty())", self.telemetry)
        self.assertIn("REVERTED_CARRIER_REMAINS_DOWN", self.telemetry)
        self.assertIn("e.end_ns <= e.start_ns", self.telemetry)
        self.assertIn("loaded_fault_ids_.insert(e.fault_id)", self.telemetry)

    def test_alarm_deduplication_is_per_fault_not_permanent_per_link(self) -> None:
        observe = function_body(self.telemetry, "void ObserveHardDisconnect")
        self.assertIn("alarmed_fault_ids_.count(fault_id)", observe)
        self.assertNotIn("alarmed_links_", observe)

    def test_detection_can_run_while_recovery_action_is_disabled(self) -> None:
        self.assertIn('"LIMER_RECOVERY_ACTION_ENABLE", false', self.telemetry)
        consume = function_body(self.telemetry, "void ConsumeAlarm")
        self.assertLess(
            consume.index("if (!recovery_action_enabled_)"),
            consume.index("RecomputeRoutesAfterLinkStateChange"),
        )
        self.assertIn('"disabled_by_experiment"', consume)


class MtpFaultBarrierContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.interface_header = (MTP / "mtp-interface.h").read_text(
            encoding="utf-8"
        )
        cls.interface_source = (MTP / "mtp-interface.cc").read_text(
            encoding="utf-8"
        )
        cls.logical_source = (MTP / "logical-process.cc").read_text(
            encoding="utf-8"
        )
        cls.multithreaded = (MTP / "multithreaded-simulator-impl.cc").read_text(
            encoding="utf-8"
        )
        cls.hybrid = (MPI / "hybrid-simulator-impl.cc").read_text(
            encoding="utf-8"
        )
        cls.telemetry = TELEMETRY.read_text(encoding="utf-8")

    def test_only_fault_injector_apply_and_revert_use_priority_api(self) -> None:
        init = function_body(self.telemetry, "void Init()")
        self.assertEqual(
            init.count("ns3::MtpInterface::ScheduleFaultBarrier("), 2
        )
        self.assertIn("&FaultInjector::Apply", init)
        self.assertIn("&FaultInjector::Revert", init)
        self.assertEqual(
            self.telemetry.count("ns3::MtpInterface::ScheduleFaultBarrier("), 2
        )
        self.assertIn("#ifdef NS3_MTP", init)
        self.assertEqual(init.count("ns3::Simulator::Schedule("), 2)

    def test_fault_api_accepts_member_callbacks_and_has_no_event_id(self) -> None:
        marker = "ScheduleFaultBarrier (Time const &delay, FUNC f"
        declaration = self.interface_header[
            self.interface_header.index("// LIMER fault injection") :
            self.interface_header.index("static void ClearFaultBarrierEvents")
        ]
        self.assertIn(">::type =", declaration)
        self.assertIn(marker, declaration)
        self.assertIn("inline static void", declaration)
        self.assertNotIn("EventId\n  ScheduleFaultBarrier", declaration)

    def test_ordinary_private_window_remains_inclusive(self) -> None:
        ordinary = function_body(
            self.logical_source, "LogicalProcess::ProcessOneRound ()"
        )
        priority_aware = function_body(
            self.logical_source,
            "LogicalProcess::ProcessOneRoundWithFaultBarrier ()",
        )
        self.assertIn("while (Next () <= grantedTime)", ordinary)
        self.assertNotIn("GetNextFaultBarrierTime", ordinary)
        self.assertIn("Next () < grantedTime", priority_aware)
        self.assertIn("Next () <= grantedTime", priority_aware)
        self.assertIn("stoppedByFaultBarrier", priority_aware)

    def test_fault_private_public_phase_order_and_boundary_refresh(self) -> None:
        body = function_body(
            self.interface_source, "MtpInterface::ProcessOneRound ()"
        )
        first_private = body.index("ProcessPrivateStage")
        fault = body.index("ProcessFaultBarrierEvents")
        refresh = body.index("g_nextPublicTime = g_systems[0].Next ()", fault)
        second_private = body.index("ProcessPrivateStage", fault)
        ordinary_public = body.index("g_systems[0].ProcessOneRound", second_private)
        self.assertLess(first_private, fault)
        self.assertLess(fault, refresh)
        self.assertLess(refresh, second_private)
        self.assertLess(second_private, ordinary_public)

    def test_fault_queue_participates_in_time_and_finish_calculation(self) -> None:
        body = function_body(
            self.interface_source, "MtpInterface::CalculateSmallestTime ()"
        )
        self.assertIn("GetNextFaultBarrierTime () < g_smallestTime", body)
        self.assertIn("!HasPendingFaultBarrierEvents ()", body)
        self.assertIn("std::atomic<uint64_t> g_nextFaultBarrierTimeStep", self.interface_header)

    def test_priority_callbacks_do_not_expire_same_time_public_events(self) -> None:
        body = function_body(
            self.interface_source, "MtpInterface::ProcessFaultBarrierEvents ()"
        )
        self.assertIn("event.key.m_uid = EventId::UID::INVALID", body)
        self.assertIn("g_systems[0].InvokeNow (event)", body)

    def test_stop_destroy_clear_but_auto_partition_preserves_queue(self) -> None:
        for source, simulator in (
            (self.multithreaded, "MultithreadedSimulatorImpl"),
            (self.hybrid, "HybridSimulatorImpl"),
        ):
            stop = function_body(source, f"{simulator}::Stop (")
            destroy = function_body(source, f"{simulator}::Destroy ()")
            partition = function_body(source, f"{simulator}::Partition ()")
            self.assertIn("MtpInterface::ClearFaultBarrierEvents ()", stop)
            self.assertIn("MtpInterface::ClearFaultBarrierEvents ()", destroy)
            self.assertNotIn("ClearFaultBarrierEvents", partition)


if __name__ == "__main__":
    unittest.main()
