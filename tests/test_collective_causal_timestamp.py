import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
FRONTEND = (
    ROOT / "astra-sim-alibabacloud/astra-sim/network_frontend/ns3"
)
CAUSAL_TIME = FRONTEND / "collective_causal_time.h"
TELEMETRY = FRONTEND / "limer_telemetry.h"


class CollectiveCausalTimestampTests(unittest.TestCase):
    def test_forward_and_reverse_lp_callback_order(self) -> None:
        source = textwrap.dedent(
            r"""
            #include "collective_causal_time.h"
            #include <cstdint>
            #include <iostream>

            int main() {
              // Normal callback order: the last callback's LP clock is also
              // the greatest timestamp.
              uint64_t forward_max = 0;
              forward_max = limer::CollectiveReadyMaximum(forward_max, 101);
              forward_max = limer::CollectiveReadyMaximum(forward_max, 109);
              if (forward_max != 109) return 1;
              if (limer::CollectiveCommitTimestamp(110, forward_max) != 110)
                return 2;

              // Reverse MTP clock order: the callback that runs last reports
              // an older LP-local time.  COMMIT must remain at the maximum
              // previously recorded READY timestamp.
              uint64_t reverse_max = 0;
              reverse_max = limer::CollectiveReadyMaximum(reverse_max, 209);
              reverse_max = limer::CollectiveReadyMaximum(reverse_max, 204);
              if (reverse_max != 209) return 3;
              if (limer::CollectiveCommitTimestamp(204, reverse_max) != 209)
                return 4;
              if (limer::CollectiveCommitDelay(204, 209) != 5)
                return 7;
              if (limer::CollectiveCommitDelay(209, 209) != 0)
                return 8;

              // A new attempt starts with an explicit zero accumulator and
              // cannot inherit the old attempt's timestamp.
              uint64_t new_attempt_max = 0;
              new_attempt_max =
                  limer::CollectiveReadyMaximum(new_attempt_max, 301);
              if (new_attempt_max != 301) return 5;
              if (limer::CollectiveCommitTimestamp(302, new_attempt_max) !=
                  302)
                return 6;

              std::cout << "forward=" << forward_max
                        << " reverse=" << reverse_max
                        << " new_attempt=" << new_attempt_max << '\n';
              return 0;
            }
            """
        )
        with tempfile.TemporaryDirectory(prefix="limer-causal-time-") as tmp:
            tmp_path = Path(tmp)
            source_path = tmp_path / "causal_time_test.cc"
            binary_path = tmp_path / "causal_time_test"
            source_path.write_text(source, encoding="utf-8")
            compiled = subprocess.run(
                [
                    "g++", "-std=c++17", "-O2", "-I", str(FRONTEND),
                    str(source_path), "-o", str(binary_path),
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(
                compiled.returncode, 0, compiled.stdout + compiled.stderr
            )
            ran = subprocess.run(
                [str(binary_path)], check=False, capture_output=True,
                text=True, timeout=10,
            )
            self.assertEqual(ran.returncode, 0, ran.stdout + ran.stderr)
            self.assertIn("forward=109 reverse=209 new_attempt=301", ran.stdout)

    def test_telemetry_uses_attempt_scoped_causal_timestamps(self) -> None:
        source = TELEMETRY.read_text(encoding="utf-8")
        local_complete = source.split(
            "bool CollectiveLocalComplete(AstraSim::DataSet* dataset)", 1
        )[1].split("void CollectiveStarted(AstraSim::DataSet* dataset)", 1)[0]
        abort = source.split("void AbortInFlightCollectives()", 1)[1].split(
            "void StopAtObservationHorizon()", 1
        )[0]

        self.assertIn('#include "collective_causal_time.h"', source)
        self.assertEqual(
            local_complete.count("const uint64_t ready_timestamp_ns ="), 1
        )
        self.assertIn(
            "CollectiveReadyMaximum(\n        state.max_ready_timestamp_ns, "
            "ready_timestamp_ns)",
            local_complete,
        )
        self.assertIn(
            "AppendCollectiveTxAt(ready_timestamp_ns", local_complete
        )
        self.assertIn(
            "CollectiveCommitTimestamp(\n          "
            "ready_timestamp_ns, state.max_ready_timestamp_ns)",
            local_complete,
        )
        self.assertIn(
            "CollectiveCommitDelay(\n          ready_timestamp_ns, "
            "commit_timestamp_ns)",
            local_complete,
        )
        self.assertIn(
            "&TelemetryCollector::ReleaseCollective, this",
            local_complete,
        )
        self.assertIn(
            "ns3::Simulator::Schedule(ns3::NanoSeconds("
            "commit_release_delay_ns)",
            local_complete,
        )
        self.assertNotIn("ns3::Simulator::ScheduleNow(", local_complete)

        release = source.split(
            "void ReleaseCollective(uint64_t sequence, "
            "uint64_t scheduled_attempt)", 1
        )[1].split("void AbortInFlightCollectives()", 1)[0]
        self.assertIn(
            "if (now_ns < state.max_ready_timestamp_ns)", release
        )
        self.assertIn(
            'AppendCollectiveTxAt(now_ns, sequence, state.attempt, "COMMIT"',
            release,
        )
        self.assertIn("state.commit_released = true;", release)
        self.assertIn("found->second.attempt != scheduled_attempt", release)
        self.assertIn(
            "found->second.started.size() != true_gpu_count_", release
        )
        self.assertIn("ns3::Simulator::ScheduleNow(", release)
        self.assertLess(
            release.index('AppendCollectiveTxAt(now_ns, sequence'),
            release.index("ns3::Simulator::ScheduleNow("),
        )
        self.assertIn("uint64_t max_ready_timestamp_ns = 0;", source)
        self.assertIn("state.max_ready_timestamp_ns = 0;", abort)


if __name__ == "__main__":
    unittest.main()
