#include "limer_split_policy.h"
#include "limer_sparse_signal.h"
#include <cassert>
#include <cmath>
#include <iostream>
#include <limits>
#include <stdexcept>

using limer::SparseEgressSignal;
using limer::SplitPolicy;
static void Seed(SplitPolicy& p, unsigned src = 0,
                 uint64_t a_ns = 100, uint64_t b_ns = 200) {
  for (unsigned r = 0; r < 2; ++r) {
    p.Reserve(src, 4, r, 1000);
    p.ChunkComplete(src, r, 1000, r ? b_ns : a_ns);
    p.Complete(src, 4, r, 1000);
  }
}
static bool Near(double a, double b) {
  return std::abs(a - b) <= 1e-6 * std::max(1.0, std::abs(b));
}
template <class Fn> static void MustThrow(Fn fn) {
  bool threw = false;
  try { fn(); } catch (const std::invalid_argument&) { threw = true; }
  assert(threw);
}

int main() {
  for (const char* name : {"B10", "B11", "B12"}) {
    SplitPolicy p(name);
    assert(p.Adaptive() && p.ChunkFeedback());
    assert(p.WaitFactor(0, 0) == 1.0);
    Seed(p);
    assert(Near(p.Rate(0, 4, 0, 1000), 80e9));
    assert(Near(p.Rate(0, 4, 1, 1000), 40e9));
    p.wait_factor[{0, 0}] = 0.4;
    assert(Near(p.Rate(0, 4, 0, 1000), 32e9));
    assert(Near(p.Rate(0, 4, 1, 1000), 40e9));
    Seed(p, 1);
    assert(Near(p.Rate(1, 4, 0, 1000), 80e9));
    // Oracle or instantaneous queue pollution cannot influence these policies.
    p.capacity[{0, 4, 0}] = {{0, std::numeric_limits<double>::quiet_NaN()}};
    p.capacity[{0, 4, 1}] = {{0, 0.0}};
    p.state[{0, 0}].queue = std::numeric_limits<uint64_t>::max();
    p.destination_queue[{4, 1}] = std::numeric_limits<uint64_t>::max();
    p.destination_reserved[{4, 1}] = std::numeric_limits<uint64_t>::max();
    assert(Near(p.Rate(0, 4, 0, 1000), 32e9));
    assert(Near(p.Rate(0, 4, 1, 1000), 40e9));
  }

  SplitPolicy causal("B12");
  Seed(causal);
  causal.DeliverSignal(0, 4, 0, 100, 120, 0.3);
  assert(causal.SignalFactor(0, 4, 0, 119) == 1.0);
  assert(causal.SignalFactor(0, 4, 0, 120) == 0.3);
  assert(causal.SignalFactor(0, 4, 0, 50100) == 0.3);
  assert(causal.SignalFactor(0, 4, 0, 50101) == 1.0);
  assert(causal.SignalFactor(1, 4, 0, 120) == 1.0);  // Observer isolation.
  assert(causal.SignalFactor(0, 5, 0, 120) == 1.0);  // Destination isolation.
  assert(causal.SignalFactor(0, 4, 1, 120) == 1.0);  // Rail isolation.
  assert(causal.SignalFactor(4, 0, 0, 120) == 1.0);  // Not a reverse signal.
  causal.DeliverSignal(0, 4, 0, 90, 130, 0.9);
  causal.DeliverSignal(0, 4, 0, 100, 130, 0.9);
  assert(causal.SignalFactor(0, 4, 0, 130) == 0.3);  // Older/equal rejected.
  causal.wait_factor[{0, 0}] = 0.4;
  assert(Near(causal.Rate(0, 4, 0, 130), 24e9));  // min, not product.
  causal.wait_factor[{0, 0}] = 0.2;
  assert(Near(causal.Rate(0, 4, 0, 130), 16e9));
  causal.DeliverSignal(0, 4, 0, 131, 140, 1.0);
  assert(causal.SignalFactor(0, 4, 0, 140) == 1.0);
  assert(Near(causal.Rate(0, 4, 0, 140), 16e9));
  MustThrow([&]() { causal.DeliverSignal(0, 4, 0, 200, 100, 0.3); });
  MustThrow([&]() { causal.DeliverSignal(0, 4, 0, 200, 200, -0.1); });
  MustThrow([&]() { causal.DeliverSignal(0, 4, 0, 200, 200, 1.1); });
  MustThrow([&]() { causal.DeliverSignal(0, 4, 0, 200, 200,
                         std::numeric_limits<double>::quiet_NaN()); });
  for (const char* name : {"B10", "B11"}) {
    SplitPolicy endpoint(name);
    Seed(endpoint);
    endpoint.DeliverSignal(0, 4, 0, 100, 120, 0.1);
    assert(endpoint.SignalFactor(0, 4, 0, 120) == 1.0);
    assert(Near(endpoint.Rate(0, 4, 0, 120), 80e9));
  }

  SplitPolicy queued("B11");
  Seed(queued);
  queued.Reserve(0, 4, 0, 8000);
  queued.Reserve(0, 4, 1, 1000);
  assert(queued.Select(0, 4, 1000, 1000) == 1);  // Faster rail has backlog.
  queued.destination_reserved[{4, 1}] = std::numeric_limits<uint64_t>::max();
  queued.destination_queue[{4, 1}] = std::numeric_limits<uint64_t>::max();
  queued.state[{0, 1}].queue = std::numeric_limits<uint64_t>::max();
  queued.capacity[{0, 4, 1}] = {{0, 0.0}};
  assert(queued.Select(0, 4, 1000, 1000) == 1);
  queued.state[{0, 1}].up = false;
  assert(queued.Select(0, 4, 1000, 1000) == 0);
  queued.state[{0, 0}].up = false;
  assert(queued.Select(0, 4, 1000, 1000) == -1);

  SplitPolicy endpoint("B11"), hybrid("B12");
  Seed(endpoint); Seed(hybrid);
  endpoint.wait_factor[{0, 0}] = hybrid.wait_factor[{0, 0}] = 0.6;
  hybrid.DeliverSignal(0, 4, 0, 100, 120, 1.0);
  hybrid.DeliverSignal(0, 4, 1, 100, 120, 1.0);
  for (unsigned i = 0; i < 100; ++i) {
    const int a = endpoint.Select(0, 4, 1000, 200 + i);
    const int b = hybrid.Select(0, 4, 1000, 200 + i);
    assert(a == b);
    endpoint.Reserve(0, 4, a, 1000);
    hybrid.Reserve(0, 4, b, 1000);
  }
  assert(endpoint.chunk_credit.empty() && hybrid.chunk_credit.empty());

  SplitPolicy credit("B10");
  Seed(credit, 0, 100, 300);  // 3:1 measured rates.
  credit.wait_factor[{0, 0}] = 0.5;  // Effective 1.5:1 -> 60:40.
  unsigned a_count = 0;
  for (unsigned i = 0; i < 100; ++i) {
    const int r = credit.Select(0, 4, 1000, 1000 + i);
    a_count += r == 0;
    credit.Reserve(0, 4, r, 1000);
  }
  assert(a_count == 60 && !credit.chunk_credit.empty());

  static_assert(sizeof(SparseEgressSignal) <= 48, "sparse port state budget");
  SparseEgressSignal sparse;
  double bps = -1;
  bool demand = true;
  assert(sparse.Sample(0, 0, 1, true, 1000, bps, demand));
  assert(!demand && bps == 0 && sparse.factor == 1.0f);
  assert(!sparse.Sample(10, 100, 1, true, 1000, bps, demand));
  assert(demand && Near(bps, 80e9) && Near(sparse.reference_bps, 80e9));
  assert(!sparse.Sample(20, 150, 1, true, 1000, bps, demand));
  assert(sparse.low_windows == 1 && sparse.factor == 1.0f);
  assert(sparse.Sample(30, 200, 1, true, 1000, bps, demand));
  assert(sparse.low_windows == 2 && Near(sparse.factor, 0.5));
  assert(!sparse.Sample(40, 250, 1, true, 1000, bps, demand));
  assert(sparse.Sample(50, 300, 0, true, 1000, bps, demand));
  assert(!demand && sparse.factor == 1.0f && sparse.low_windows == 0);
  assert(!sparse.Sample(60, 350, 1, true, 1000, bps, demand));
  assert(!demand && sparse.factor == 1.0f);  // Both endpoints need a queue.
  assert(!sparse.Sample(70, 450, 1, true, 1000, bps, demand));
  assert(demand && sparse.factor == 1.0f);
  assert(sparse.Sample(80, 450, 1, false, 1000, bps, demand));
  assert(Near(sparse.factor, 0.1) && sparse.low_windows == 0);
  assert(sparse.Sample(90, 450, 0, true, 1000, bps, demand));
  assert(!demand && sparse.factor == 1.0f);
  assert(!sparse.Sample(1089, 450, 0, true, 1000, bps, demand));
  assert(sparse.Sample(1090, 450, 0, true, 1000, bps, demand));
  assert(sparse.last_sent_ns == 1090);
  MustThrow([&]() { sparse.Sample(1090, 450, 0, true, 1000, bps, demand); });
  MustThrow([&]() { sparse.Sample(1091, 449, 0, true, 1000, bps, demand); });

  SparseEgressSignal cold_down;
  assert(cold_down.Sample(0, 0, 0, false, 1000, bps, demand));
  assert(Near(cold_down.factor, 0.1));  // Down is actionable at first sample.
  std::cout << "adaptive policy and sparse switch signal tests passed\n";
}
