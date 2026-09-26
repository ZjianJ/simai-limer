#include "limer_split_policy.h"
#include <cassert>
#include <cmath>
#include <iostream>
#include <stdexcept>
#include <string>

using limer::CensoredModel;
using limer::SplitPolicy;
static bool Near(double a, double b) { return std::abs(a - b) < 1e-10; }
static CensoredModel::Evidence Band(double lower, double upper, bool ready = true) {
  CensoredModel::Evidence evidence;
  evidence.lower = lower;
  evidence.upper = upper;
  evidence.ready = ready;
  return evidence;
}
static SplitPolicy Candidate(const std::string& name = "B14", unsigned src = 0) {
  SplitPolicy policy(name);
  for (unsigned rail = 0; rail < 2; ++rail) {
    policy.chunk_estimate[{src, rail}].launched = 1;
    policy.chunk_estimate[{src, rail}].samples = 1;
    policy.chunk_estimate[{src, rail}].bps = rail ? 20e9 : 80e9;
  }
  policy.censored_evidence[{src, 0}] = Band(.6, .8);
  policy.censored_evidence[{src, 1}] = Band(0, 1, false);
  policy.exploration[src].total_bytes = 1500;
  return policy;
}
static void NoProbe(SplitPolicy policy, uint64_t now = 50000,
                    uint64_t queued = 8, unsigned src = 0, unsigned dst = 5) {
  const int selected = policy.Select(src, dst, 100, now, queued);
  assert(selected == 0);
  assert(!policy.censored_choice[src].probe);
  assert(policy.censored_choice[src].base == 0);
}

int main() {
  SplitPolicy factors("B13");
  assert(factors.Censored() && factors.ChunkFeedback() && !factors.Adaptive());
  assert(factors.CensoredFactor(0, 0) == 1);
  factors.censored_evidence[{0, 0}] = Band(.4, .7);
  factors.censored_evidence[{0, 1}] = Band(.6, .8);
  assert(factors.CensoredFactor(0, 0) == 1);  // Overlap preserves B8.
  assert(factors.CensoredFactor(0, 1) == 1);
  factors.censored_evidence[{0, 0}] = Band(.1, .3);
  assert(Near(factors.CensoredFactor(0, 0), .5));
  assert(factors.CensoredFactor(0, 1) == 1);
  factors.censored_evidence[{0, 0}] = Band(0, .01);
  assert(factors.CensoredFactor(0, 0) == .1);  // Floor preserves useful traffic.
  factors.censored_evidence[{0, 1}].ready = false;
  assert(factors.CensoredFactor(0, 0) == 1);  // Cold evidence cannot penalize.
  factors.censored_evidence[{0, 1}].ready = true;
  factors.censored_actuate = false;
  assert(factors.CensoredFactor(0, 0) == 1);
  factors.censored_actuate = true;
  factors.chunk_estimate[{0, 0}].bps = 20e9;
  assert(factors.Rate(0, 5, 0, 0) == 2e9);

  auto allowed = Candidate();
  assert(allowed.Select(0, 5, 100, 50000, 8) == 1);
  assert(allowed.censored_choice[0].probe && allowed.censored_choice[0].base == 0);
  assert(allowed.censored_choice[0].queued == 8);
  // Credit is debited to the ACTUAL selected rail, not the displaced base rail.
  assert(Near(allowed.chunk_credit[{0, 5}][0], 80));
  assert(Near(allowed.chunk_credit[{0, 5}][1], -80));
  allowed.CensoredAssigned(0, 100, 50000);
  assert(allowed.exploration[0].total_bytes == 1600);
  assert(allowed.exploration[0].probe_bytes == 100);
  assert(allowed.exploration[0].inflight == 1);
  assert(allowed.exploration[0].last_probe_ns == 50000);
  allowed.CensoredProbeComplete(0);
  assert(!allowed.exploration[0].inflight);
  bool underflow = false;
  try { allowed.CensoredProbeComplete(0); }
  catch (const std::runtime_error&) { underflow = true; }
  assert(underflow);

  NoProbe(Candidate("B13"));  // Confidence correction only; never exploration.
  auto shadow = Candidate();
  shadow.censored_actuate = false;
  NoProbe(shadow);
  NoProbe(Candidate(), 49999);  // Source cooldown includes initial start.
  NoProbe(Candidate(), 50000, 7);  // Locally queued tail guard.
  auto recent_ack = Candidate();
  recent_ack.censored_last_ack[{0, 1}] = 1;
  NoProbe(recent_ack);  // Target ACK must itself be stale for 50 us.
  auto busy_target = Candidate();
  busy_target.state[{0, 1}].reserved = 1;
  NoProbe(busy_target);
  auto down_target = Candidate();
  down_target.state[{0, 1}].up = false;
  NoProbe(down_target);
  auto insufficient = Candidate();
  insufficient.exploration[0].total_bytes = 1499;
  NoProbe(insufficient);  // Inclusive exact byte budget, not rounded chunks.
  auto outstanding = Candidate();
  outstanding.exploration[0].inflight = 1;
  NoProbe(outstanding, 50000, 8, 0, 6);  // Different dst cannot evade source cap.
  auto cooldown = Candidate();
  cooldown.exploration[0].last_probe_ns = 1;
  NoProbe(cooldown);
  auto unpromising = Candidate();
  unpromising.censored_evidence[{0, 1}] = Band(.1, .5);
  NoProbe(unpromising);  // Target UCB below incumbent LCB.
  auto equal_bound = Candidate();
  equal_bound.censored_evidence[{0, 1}] = Band(.1, .6);
  assert(equal_bound.Select(0, 5, 100, 50000, 8) == 1);  // Equality may explore.

  // Another source has its own budget/cooldown/outstanding-probe state.
  auto independent = Candidate("B14", 1);
  independent.exploration[0].inflight = 1;
  assert(independent.Select(1, 5, 100, 50000, 8) == 1);
  assert(independent.censored_choice[1].probe);

  // Variable useful-data chunk lengths obey the cumulative 1/16 byte bound
  // after every assignment; a completed probe merely clears outstanding state.
  auto budget = Candidate();
  uint64_t probes = 0;
  for (uint64_t i = 1; i <= 500; ++i) {
    const uint64_t bytes = 37 + (i * 29) % 101;
    const uint64_t now = i * 50000;
    const int rail = budget.Select(0, 5, bytes, now, 100);
    assert(rail == 0 || rail == 1);
    const bool probe = budget.censored_choice[0].probe;
    budget.CensoredAssigned(0, bytes, now);
    assert(budget.exploration[0].probe_bytes * 16 <= budget.exploration[0].total_bytes);
    assert(budget.exploration[0].inflight == (probe ? 1u : 0u));
    if (probe) {
      ++probes;
      budget.CensoredProbeComplete(0);
    }
  }
  assert(probes > 0);

  // Observation-only B14 must match B8 exactly despite intentionally separated
  // bands and enough budget/stale time to permit probes if actuation were on.
  auto b8 = Candidate("B8");
  auto no_actuation = Candidate();
  no_actuation.censored_actuate = false;
  no_actuation.censored_evidence[{0, 0}] = Band(0, .01);
  no_actuation.censored_evidence[{0, 1}] = Band(.8, .9);
  for (uint64_t i = 1; i <= 500; ++i) {
    const uint64_t bytes = 37 + i % 101;
    const uint64_t now = i * 50000;
    assert(b8.Select(0, 5, bytes, now) ==
           no_actuation.Select(0, 5, bytes, now, 100));
    assert((b8.chunk_credit[{0, 5}] == no_actuation.chunk_credit[{0, 5}]));
    assert(!no_actuation.censored_choice[0].probe);
    no_actuation.CensoredAssigned(0, bytes, now);
    if (i % 7 == 0) {
      const unsigned rail = static_cast<unsigned>(i % 2);
      b8.ChunkComplete(0, rail, bytes, 1000 + i);
      no_actuation.ChunkComplete(0, rail, bytes, 1000 + i);
    }
  }
  assert(no_actuation.exploration[0].probe_bytes == 0);

  // Initial mandatory two-rail probes are ordinary B8 behavior, not UCB budget
  // spending. Their payload is still counted in the source's total assignment.
  SplitPolicy initial("B14");
  assert(initial.Select(0, 5, 100, 0, 100) == 0);
  assert(!initial.censored_choice[0].probe && initial.censored_choice[0].base == -1);
  initial.Reserve(0, 5, 0, 100);
  initial.CensoredAssigned(0, 100, 0);
  assert(initial.Select(0, 5, 100, 0, 100) == 1);
  initial.Reserve(0, 5, 1, 100);
  initial.CensoredAssigned(0, 100, 0);
  assert(initial.exploration[0].total_bytes == 200);
  assert(initial.exploration[0].probe_bytes == 0);
  std::cout << "censored confidence and bounded UCB policy tests passed\n";
}
