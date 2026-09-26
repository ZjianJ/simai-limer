#include <cmath>
#include <cstdlib>
#include <iostream>
#include <iterator>
#include <string>
#include <utility>
#include <vector>

#include "astra-sim/system/UsageTracker.hh"

namespace {

using AstraSim::UsageTracker;

std::vector<std::pair<uint64_t, double>> AsVector(
    const std::list<std::pair<uint64_t, double>>& values) {
  return {values.begin(), values.end()};
}

void Require(bool condition, const std::string& message) {
  if (!condition) {
    std::cerr << message << std::endl;
    std::exit(1);
  }
}

void RequirePercentages(
    const std::vector<std::pair<uint64_t, double>>& actual,
    const std::vector<std::pair<uint64_t, double>>& expected) {
  Require(actual.size() == expected.size(), "unexpected result length");
  for (size_t i = 0; i < actual.size(); ++i) {
    Require(actual[i].first == expected[i].first, "unexpected period end");
    Require(
        std::abs(actual[i].second - expected[i].second) < 1e-12,
        "unexpected utilization");
  }
}

}  // namespace

int main(int argc, char** argv) {
  if (argc == 2 && std::string(argv[1]) == "invalid-reversed-interval") {
    UsageTracker tracker(2);
    tracker.usage.emplace_back(1, 10, 5);
    tracker.last_tick = 5;
    tracker.report_percentage_at(10, 10);
    return 0;
  }
  if (argc == 2 && std::string(argv[1]) == "oversized-clock-regression") {
    UsageTracker tracker(2);
    tracker.last_tick =
        UsageTracker::kMaxToleratedClockRegressionTicks + 1;
    tracker.report_percentage_at(10, 0);
    return 0;
  }

  {
    UsageTracker tracker(2);
    tracker.usage.emplace_back(0, 0, 5000);
    tracker.usage.emplace_back(1, 5000, 15000);
    tracker.current_level = 0;
    tracker.last_tick = 15000;
    const size_t history_size = tracker.usage.size();
    RequirePercentages(
        AsVector(tracker.report_percentage_at(10000, 20000)),
        {{10000, 50.0}, {20000, 50.0}});
    Require(tracker.current_level == 0, "report changed current level");
    Require(tracker.last_tick == 15000, "report changed last transition tick");
    Require(tracker.usage.size() == history_size, "report changed history");
  }

  {
    // This is the preserved production failure shape: the report callback is
    // processed after a transition but exposes an earlier MTP timestamp.
    UsageTracker tracker(2);
    tracker.usage.emplace_back(1, 0, 1126451);
    tracker.current_level = 0;
    tracker.last_tick = 1126451;
    const auto result = tracker.report_percentage_at(10000, 1126436);
    Require(result.size() == 112, "regressed report lost complete periods");
    Require(tracker.clock_regression_count == 1, "regression was not counted");
    Require(tracker.max_clock_regression == 15, "wrong regression magnitude");
    Require(tracker.current_level == 0, "regressed report changed level");
    Require(tracker.last_tick == 1126451, "regressed report moved clock back");
  }

  {
    UsageTracker tracker(2);
    tracker.usage.emplace_back(1, 0, 5000);
    tracker.usage.emplace_back(1, 15000, 20000);
    tracker.current_level = 0;
    tracker.last_tick = 20000;
    RequirePercentages(
        AsVector(tracker.report_percentage_at(10000, 20000)),
        {{10000, 50.0}, {20000, 50.0}});
  }

  {
    UsageTracker tracker(2);
    tracker.usage.emplace_back(0, 0, 5000);
    tracker.usage.emplace_back(1, 5000, 25000);
    tracker.current_level = 0;
    tracker.last_tick = 25000;
    RequirePercentages(
        AsVector(tracker.report_percentage_at(10000, 30000)),
        {{10000, 50.0}, {20000, 100.0}, {30000, 50.0}});
  }

  return 0;
}
