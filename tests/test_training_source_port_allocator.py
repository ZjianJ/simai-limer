import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
ALLOCATOR = (
    ROOT
    / "astra-sim-alibabacloud/astra-sim/network_frontend/ns3"
    / "training_source_port_allocator.h"
)
ENTRY = (
    ROOT
    / "astra-sim-alibabacloud/astra-sim/network_frontend/ns3/entry.h"
)
COMMON = (
    ROOT
    / "astra-sim-alibabacloud/astra-sim/network_frontend/ns3/common.h"
)
RDMA_HW = (
    ROOT
    / "ns-3-alibabacloud/simulation/src/point-to-point/model/rdma-hw.cc"
)
RDMA_QP = (
    ROOT
    / "ns-3-alibabacloud/simulation/src/point-to-point/model/rdma-queue-pair.h"
)


class TrainingSourcePortAllocatorTests(unittest.TestCase):
    def test_frontend_lifecycle_contract_is_explicit(self) -> None:
        entry = ENTRY.read_text(encoding="utf-8")
        common = COMMON.read_text(encoding="utf-8")
        rdma_hw = RDMA_HW.read_text(encoding="utf-8")
        rdma_qp = RDMA_QP.read_text(encoding="utf-8")

        self.assertNotIn("portNumber[src][dst]++", entry)
        self.assertNotIn("portNumber", common)
        self.assertIn("LIMER_TRAINING_SPORT_MIN = 10000", rdma_qp)
        self.assertIn("LIMER_BACKGROUND_SPORT_MIN = 49152", rdma_qp)
        self.assertIn("Simulator::ScheduleWithContext", entry)
        self.assertIn("release ran before sender QP deletion", entry)
        self.assertIn("driver->m_rdma->GetQp", entry)
        self.assertIn("sender_src_port_map.count(sender_key)", entry)
        self.assertIn("MtpInterface::explicitCriticalSection", entry)
        self.assertIn("training_source_port_allocator.csv", entry)
        self.assertIn("external_conflicts", entry)
        self.assertIn("invariant_errors", entry)
        self.assertIn("RDMA QP key collision before install", rdma_hw)
        self.assertIn("RDMA QP deletion found a different QP", rdma_hw)

    def test_long_run_wrap_exhaustion_and_concurrency(self) -> None:
        source = textwrap.dedent(
            r"""
            #include "training_source_port_allocator.h"
            #include <atomic>
            #include <cstdint>
            #include <iostream>
            #include <mutex>
            #include <set>
            #include <string>
            #include <thread>
            #include <vector>

            using limer::TrainingSourcePortAllocator;

            int main() {
              TrainingSourcePortAllocator long_run(10000, 49152);
              const uint32_t capacity = long_run.Capacity();
              if (capacity != 39152) return 1;
              for (uint32_t i = 0; i < capacity + 137; ++i) {
                auto a = long_run.Reserve(3, 7, 3, {});
                if (!a.ok) return 2;
                if (a.port < 10000 || a.port >= 49152) return 3;
                if (i < capacity && a.reused) return 4;
                if (i == capacity &&
                    (!a.reused || !a.first_reuse_for_pair || a.port != 10000 ||
                     a.allocation_ordinal != capacity + 1)) return 5;
                std::string error;
                if (!long_run.Release(3, 7, a.port, 3, &error)) return 6;
              }
              auto long_stats = long_run.GetSnapshot();
              if (long_stats.total_allocations != capacity + 137) return 7;
              if (long_stats.total_releases != capacity + 137) return 24;
              if (long_stats.total_reuses != 137) return 8;
              if (long_stats.active_reservations != 0) return 9;
              if (long_stats.min_allocated_port != 10000 ||
                  long_stats.max_allocated_port != 49151) return 10;
              if (long_stats.external_conflicts != 0 ||
                  long_stats.exhaustions != 0 ||
                  long_stats.invariant_errors != 0) return 11;
              if (long_stats.pairs_with_reuse != 1 ||
                  long_stats.max_pair_allocations != capacity + 137 ||
                  long_stats.max_pair_reuses != 137) return 25;

              TrainingSourcePortAllocator full(100, 104);
              std::vector<uint16_t> held;
              for (int i = 0; i < 4; ++i) {
                auto a = full.Reserve(1, 2, 3, {});
                if (!a.ok) return 12;
                held.push_back(a.port);
              }
              auto exhausted = full.Reserve(1, 2, 3, {});
              if (exhausted.ok || full.GetSnapshot().exhaustions != 1) return 13;
              std::string wrong_error;
              if (full.Release(1, 2, held[0], 4, &wrong_error)) return 14;
              if (wrong_error.find("reserved_pg=3") == std::string::npos)
                return 15;
              if (full.GetSnapshot().invariant_errors != 1) return 16;

              TrainingSourcePortAllocator conflict(200, 204);
              auto skipped = conflict.Reserve(
                  9, 10, 3, [](uint16_t port) { return port == 200; });
              if (!skipped.ok || skipped.port != 201) return 17;
              if (conflict.GetSnapshot().external_conflicts != 1) return 18;

              TrainingSourcePortAllocator concurrent(1000, 1256);
              std::mutex live_mu;
              std::set<uint16_t> live;
              std::atomic<bool> collision(false);
              std::atomic<bool> failure(false);
              std::vector<std::thread> workers;
              for (int worker = 0; worker < 8; ++worker) {
                workers.emplace_back([&]() {
                  for (int i = 0; i < 5000; ++i) {
                    auto a = concurrent.Reserve(5, 6, 3, {});
                    if (!a.ok) {
                      failure.store(true);
                      return;
                    }
                    {
                      std::lock_guard<std::mutex> guard(live_mu);
                      if (!live.insert(a.port).second) collision.store(true);
                    }
                    {
                      std::lock_guard<std::mutex> guard(live_mu);
                      live.erase(a.port);
                    }
                    std::string error;
                    if (!concurrent.Release(5, 6, a.port, 3, &error)) {
                      failure.store(true);
                      return;
                    }
                  }
                });
              }
              for (auto& worker : workers) worker.join();
              auto concurrent_stats = concurrent.GetSnapshot();
              if (failure.load() || collision.load()) return 19;
              if (!live.empty() || concurrent_stats.active_reservations != 0)
                return 20;
              if (concurrent_stats.total_allocations != 40000) return 21;
              if (concurrent_stats.total_reuses == 0) return 22;
              if (concurrent_stats.exhaustions != 0 ||
                  concurrent_stats.invariant_errors != 0) return 23;

              std::cout << "capacity=" << capacity
                        << " long_allocations=" << long_stats.total_allocations
                        << " long_reuses=" << long_stats.total_reuses
                        << " concurrent_allocations="
                        << concurrent_stats.total_allocations << '\n';
              return 0;
            }
            """
        )
        with tempfile.TemporaryDirectory(prefix="limer-sport-test-") as tmp:
            tmp_path = Path(tmp)
            source_path = tmp_path / "allocator_test.cc"
            binary_path = tmp_path / "allocator_test"
            source_path.write_text(source, encoding="utf-8")
            compile_result = subprocess.run(
                [
                    "g++",
                    "-std=c++17",
                    "-O2",
                    "-pthread",
                    "-I",
                    str(ALLOCATOR.parent),
                    str(source_path),
                    "-o",
                    str(binary_path),
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(
                compile_result.returncode,
                0,
                msg=compile_result.stdout + compile_result.stderr,
            )
            run_result = subprocess.run(
                [str(binary_path)],
                check=False,
                capture_output=True,
                text=True,
                timeout=30,
            )
            self.assertEqual(
                run_result.returncode,
                0,
                msg=run_result.stdout + run_result.stderr,
            )
            self.assertIn("capacity=39152", run_result.stdout)
            self.assertIn("long_reuses=137", run_result.stdout)


if __name__ == "__main__":
    unittest.main()
