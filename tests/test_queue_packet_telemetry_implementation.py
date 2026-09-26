#!/usr/bin/env python3
"""Static guards for exact packet-depth telemetry on Broadcom egress queues."""

from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
NETWORK = ROOT / "ns-3-alibabacloud/simulation/src/network/utils"
POINT_TO_POINT = ROOT / "ns-3-alibabacloud/simulation/src/point-to-point/model"


class QueuePacketTelemetryContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.header = (NETWORK / "broadcom-egress-queue.h").read_text(
            encoding="utf-8"
        )
        cls.source = (NETWORK / "broadcom-egress-queue.cc").read_text(
            encoding="utf-8"
        )
        cls.device = (POINT_TO_POINT / "qbb-net-device.cc").read_text(
            encoding="utf-8"
        )

    def test_shadow_counters_are_initialized(self) -> None:
        constructor = self.source[
            self.source.index("BEgressQueue::BEgressQueue()") :
            self.source.index("BEgressQueue::~BEgressQueue()")
        ]
        for counter in (
            "m_nBytes(0)",
            "m_nTotalReceivedBytes(0)",
            "m_nPackets(0)",
            "m_nTotalReceivedPackets(0)",
            "m_nTotalDroppedBytes(0)",
            "m_nTotalDroppedPackets(0)",
        ):
            self.assertIn(counter, constructor)

    def test_explicit_total_packet_accessor_uses_live_shadow_counter(self) -> None:
        self.assertIn("uint32_t GetNPacketsTotal() const;", self.header)
        start = self.source.index("BEgressQueue::GetNPacketsTotal() const")
        body = self.source[start : self.source.index("}", start) + 1]
        self.assertIn("return m_nPackets;", body)

    def test_limer_does_not_read_unused_packet_queue_base_counter(self) -> None:
        start = self.device.index("QbbNetDevice::GetLimerQueuePackets() const")
        body = self.device[start : self.device.index("}", start) + 1]
        self.assertIn("m_queue->GetNPacketsTotal()", body)
        self.assertNotIn("m_queue->GetNPackets()", body)


if __name__ == "__main__":
    unittest.main()
