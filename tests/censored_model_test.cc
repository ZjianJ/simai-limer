#include "limer_censored_model.h"
#include <cassert>
#include <cmath>
#include <cstdint>
#include <iostream>
#include <limits>
#include <stdexcept>

using limer::CensoredModel;
static bool Near(double a, double b) { return std::abs(a - b) < 1e-12; }
template <class Exception, class Function>
static void Throws(Function function) {
  bool threw = false;
  try { function(); } catch (const Exception&) { threw = true; }
  assert(threw);
}

int main() {
  CensoredModel model;
  const auto cold = model.Evaluate(0, 0, 0);
  assert(cold.lower == 0 && cold.upper == 1 && !cold.ready);
  assert(cold.count == 0 && model.ContextCount() == 0);
  assert(CensoredModel::UtilityScaleBps(1) == 100e9);
  assert(CensoredModel::UtilityScaleBps(3) == 25e9);
  assert(CensoredModel::UtilityScaleBps(4) == 25e9);
  assert(CensoredModel::UtilityScaleBps(5) == 12.5e9);
  assert(CensoredModel::UtilityScaleBps(
      std::numeric_limits<unsigned>::max()) > 0.0);

  // Eight exact records at utility .32. No pending outcome imputation.
  for (uint64_t id = 1; id <= 8; ++id)
    assert(model.Upsert(0, 0, id, 1000, 4, id * 1000, id * 1000 + 1000, true));
  const auto exact = model.Evaluate(0, 0, 20000);
  assert(exact.ready && exact.count == 8 && exact.completed == 8);
  assert(exact.pending == 0 && exact.last_completed_ns == 9000);
  assert(Near(exact.identified_lower, .32));
  assert(Near(exact.identified_upper, .32));
  assert(Near(exact.radius, std::sqrt(std::log(40.0) / 16.0)));
  assert(exact.lower == 0 && Near(exact.upper, .32 + exact.radius));

  assert(model.Upsert(0, 0, 9, 1000, 4, 20000, 20000, false));
  auto pending = model.Evaluate(0, 0, 20000);
  assert(pending.count == 9 && pending.pending == 1);
  assert(Near(pending.identified_lower, 8 * .32 / 9));
  assert(Near(pending.identified_upper, (8 * .32 + 1) / 9));
  for (uint64_t now = 20100; now <= 21000; now += 100)
    assert(model.Upsert(0, 0, 9, 1000, 4, 20000, now, false));
  auto later = model.Evaluate(0, 0, 21000);
  assert(later.count == 9 && model.RecordCount() == 9);
  assert(later.identified_lower == pending.identified_lower);
  assert(later.identified_upper < pending.identified_upper);
  assert(Near(later.identified_upper, .32));
  const auto no_pending = model.Evaluate(0, 0, 21000, false);
  assert(no_pending.count == 8 && no_pending.pending == 0);
  assert(Near(no_pending.identified_lower, .32));
  assert(model.Upsert(0, 0, 9, 1000, 4, 20000, 21100, true));
  const auto replaced = model.Evaluate(0, 0, 21100);
  assert(replaced.count == 9 && replaced.completed == 9 && !replaced.pending);
  assert(replaced.identified_lower == replaced.identified_upper);
  assert(Near(replaced.identified_lower, (8 * .32 + 320.0 / 1100.0) / 9));
  assert(model.Upsert(0, 0, 9, 1000, 4, 20000, 21100, true));
  assert(model.Evaluate(0, 0, 21100).count == 9);

  // Deterministic partial identification includes every feasible latent mean.
  CensoredModel partial;
  double eventual_sum = 0;
  for (uint64_t id = 1; id <= 8; ++id) {
    const uint64_t duration = 1000 + id * 500;
    const unsigned load = static_cast<unsigned>(id % 5 + 1);
    partial.Upsert(0, 1, id, 1000, load, 100, 1100, false);
    eventual_sum += std::min(1.0, 1000.0 * 8e9 /
        (duration * CensoredModel::UtilityScaleBps(load)));
  }
  const auto bounds = partial.Evaluate(0, 1, 1100);
  assert(bounds.identified_lower <= eventual_sum / 8);
  assert(bounds.identified_upper >= eventual_sum / 8);
  assert(bounds.identified_lower == 0 && bounds.completed == 0);
  assert(bounds.pending == 8 && bounds.ready);
  assert(partial.Evaluate(0, 0, 1100).count == 0);
  assert(partial.Evaluate(1, 1, 1100).count == 0);
  assert(partial.Evaluate(0, 1, 1100, false).upper == 1);

  // Assignment order, not completion order, determines the latest window.
  // An old live pending observation remains until completion even as it ages.
  CensoredModel window(4, 1000);
  window.Upsert(0, 0, 1, 1000, 4, 10, 10, false);
  for (uint64_t id = 2; id <= 7; ++id)
    window.Upsert(0, 0, id, 1000, 4, id * 100, id * 100 + 10, true);
  auto bounded = window.Evaluate(0, 0, 800);
  assert(bounded.count == 5 && bounded.completed == 4 && bounded.pending == 1);
  assert(window.RecordCount() == 5);
  assert(!window.Upsert(0, 0, 2, 1000, 4, 200, 210, true));
  auto aged = window.Evaluate(0, 0, 2000);
  assert(aged.count == 1 && aged.pending == 1 && aged.completed == 0);
  assert(aged.last_completed_ns == 0);
  assert(window.RecordCount() == 5);  // Query cannot change membership.
  window.Upsert(0, 0, 1, 1000, 4, 10, 2010, true);
  assert(window.RecordCount() == 4);  // Old pending does not become new history.
  assert(window.Evaluate(0, 0, 2010).count == 0);
  assert(!window.Upsert(0, 0, 2, 1000, 4, 200, 210, true));

  // An older ID first observed late is still retained while pending.
  window.Upsert(0, 0, 3, 1000, 4, 300, 2010, false);
  assert(window.Evaluate(0, 0, 2010).pending == 1);
  window.Upsert(0, 0, 3, 1000, 4, 300, 2020, true);
  assert(window.Evaluate(0, 0, 2020).count == 0);

  // Horizon is inclusive at firstTX+horizon, and then returns [0,1] cold.
  CensoredModel expires;
  expires.Upsert(0, 0, 1, 1000, 4, 100, 1100, true);
  assert(expires.Evaluate(0, 0, 250100).count == 1);
  const auto forgotten = expires.Evaluate(0, 0, 250101);
  assert(!forgotten.count && !forgotten.ready && forgotten.upper == 1);

  // More than the completed-window limit cannot grow retained state.
  CensoredModel bounded_memory(64, 250000, .05, 2);
  for (uint64_t id = 1; id <= 1000; ++id)
    bounded_memory.Upsert(0, 0, id, 1000, 4, id, id + 10, true);
  assert(bounded_memory.RecordCount() == 64);
  assert(bounded_memory.Evaluate(0, 0, 2000).count == 64);
  bounded_memory.Upsert(0, 1, 1, 1000, 1, 0, 1, true);
  assert(!bounded_memory.Upsert(1, 0, 1, 1000, 1, 0, 1, true));
  assert(bounded_memory.ContextCount() == 2);
  assert(bounded_memory.DroppedObservations() == 1);
  assert(bounded_memory.Evaluate(1, 0, 2).count == 0);

  Throws<std::length_error>([&] {
    partial.Upsert(0, 1, 9, 1000, 4, 100, 1100, false);
  });
  Throws<std::invalid_argument>([&] { CensoredModel invalid(0); });
  Throws<std::invalid_argument>([&] { CensoredModel invalid(64, 0); });
  Throws<std::invalid_argument>([&] { CensoredModel invalid(64, 1, 1); });
  Throws<std::invalid_argument>([&] { CensoredModel invalid(64, 1, 0); });
  Throws<std::invalid_argument>([&] {
    CensoredModel invalid(64, 1, std::numeric_limits<double>::quiet_NaN());
  });
  Throws<std::invalid_argument>([&] { CensoredModel::UtilityScaleBps(0); });
  Throws<std::invalid_argument>([&] { model.Evaluate(0, 2, 30000); });
  Throws<std::invalid_argument>([&] { model.Evaluate(0, 0, 1000); });
  Throws<std::invalid_argument>([&] {
    model.Upsert(0, 0, 9, 1001, 4, 20000, 21100, true);
  });
  Throws<std::invalid_argument>([&] {
    model.Upsert(0, 0, 9, 1000, 5, 20000, 21100, true);
  });
  Throws<std::invalid_argument>([&] {
    model.Upsert(0, 0, 9, 1000, 4, 20001, 21100, true);
  });
  Throws<std::invalid_argument>([&] {
    model.Upsert(0, 0, 9, 1000, 4, 20000, 21101, true);
  });
  Throws<std::invalid_argument>([&] {
    model.Upsert(0, 0, 10, 1000, 4, 30000, 30000, true);
  });
  Throws<std::invalid_argument>([&] {
    model.Upsert(0, 0, 10, 1000, 4, 30000, 29999, false);
  });
  Throws<std::invalid_argument>([&] {
    model.Upsert(0, 0, 10, 0, 4, 30000, 31000, true);
  });
  std::cout << "censored partial-identification model tests passed\n";
}
