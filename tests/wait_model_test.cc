#include "limer_wait_model.h"
#include <cassert>
#include <cmath>
#include <iostream>
#include <limits>
#include <stdexcept>

using limer::WaitModel;
static void Learn(WaitModel& model, unsigned src = 0, unsigned rail = 0,
                  uint64_t bytes = 65536, unsigned load = 4,
                  uint64_t duration = 10000, unsigned count = 8) {
  for (unsigned i = 0; i < count; ++i)
    model.Observe(src, rail, bytes, load, duration);
}

int main() {
  WaitModel model;
  assert(model.Penalty(0, 0, 65536, 4, 1000000) == 1.0);
  assert(model.ContextCount() == 0);  // Queries never allocate history.
  Learn(model, 0, 0, 65536, 4, 10000, 7);
  auto cold = model.Evaluate(0, 0, 65536, 4, 1000000);
  assert(!cold.ready && cold.samples == 7 && cold.factor == 1.0);
  model.Observe(0, 0, 65536, 4, 10000);
  auto warm = model.Evaluate(0, 0, 65536, 4, 15000);
  assert(warm.ready && warm.samples == 8 && warm.factor == 1.0);
  assert(std::abs(warm.threshold_ns - 19531.25) < 1e-7);
  assert(model.Penalty(0, 0, 65536, 4, 0) == 1.0);
  const double slow = model.Penalty(0, 0, 65536, 4, 40000);
  assert(std::abs(slow - 19531.25 / 40000) < 1e-12);
  assert(model.Penalty(0, 0, 65536, 4, 1000000) == 0.1);

  // Independent rail/source/size/load conditions; loads 3 and 4 share a bin.
  assert(model.Penalty(1, 0, 65536, 4, 1000000) == 1.0);
  assert(model.Penalty(0, 1, 65536, 4, 1000000) == 1.0);
  assert(model.Penalty(0, 0, 65537, 4, 1000000) == 1.0);
  assert(model.Penalty(0, 0, 65536, 5, 1000000) == 1.0);
  assert(model.Penalty(0, 0, 65536, 3, 1000000) == 0.1);
  Learn(model, 0, 0, 65536, 8, 40000);
  assert(model.Penalty(0, 0, 65536, 8, 60000) == 1.0);
  assert(model.Penalty(0, 0, 65536, 4, 60000) < 0.4);

  // Abrupt positive outliers do not immediately become the normal reference.
  Learn(model, 0, 0, 65536, 4, 1000000, 1000);
  auto robust = model.Evaluate(0, 0, 65536, 4, 40000);
  assert(robust.samples == 8 && robust.rejected == 1000);
  assert(robust.threshold_ns == warm.threshold_ns);
  model.Observe(0, 0, 65536, 4, 12000);  // Ordinary change still learns.
  auto ordinary = model.Evaluate(0, 0, 65536, 4, 40000);
  assert(ordinary.samples == 9 && ordinary.threshold_ns > robust.threshold_ns);
  assert(ordinary.factor < 1.0);
  Learn(model, 0, 0, 65536, 4, 10000, 100);
  assert(model.Penalty(0, 0, 65536, 4, 15000) == 1.0);

  // Completion time variation is learned rather than penalized as soon as
  // it exceeds the mean. Cold-start sampling is not a fault-free guarantee.
  WaitModel variable;
  for (unsigned i = 0; i < 8; ++i)
    variable.Observe(0, 0, 65536, 4, i % 2 ? 40000 : 10000);
  auto broad = variable.Evaluate(0, 0, 65536, 4, 50000);
  assert(broad.ready && broad.factor == 1.0 && broad.threshold_ns > 50000);

  // Huge valid observations/ages remain finite, and sizes avoid log2 casts.
  WaitModel extremes;
  const uint64_t biggest = std::numeric_limits<uint64_t>::max();
  Learn(extremes, 0, 0, biggest, 128, biggest);
  auto high = extremes.Evaluate(0, 0, biggest, 128, biggest);
  assert(high.size_bucket == 64 && high.load_bucket == 7);
  assert(std::isfinite(high.threshold_ns) && std::isfinite(high.factor));
  assert(high.factor >= 0.1 && high.factor <= 1.0);
  Learn(extremes, 1, 0, 1, 1, 1);
  assert(extremes.Penalty(1, 0, 1, 1, biggest) == 0.1);

  // A fixed context cap guarantees bounded retained model state.
  WaitModel bounded(2);
  Learn(bounded, 0);
  Learn(bounded, 1);
  Learn(bounded, 2);
  assert(bounded.ContextCount() == 2 && bounded.DroppedObservations() == 8);
  assert(!bounded.Evaluate(2, 0, 65536, 4, 1000000).ready);
  assert(bounded.Penalty(2, 0, 65536, 4, 1000000) == 1.0);

  bool threw = false;
  try { model.Observe(0, 0, 65536, 4, 0); }
  catch (const std::invalid_argument&) { threw = true; }
  assert(threw);
  threw = false;
  try { model.Penalty(0, 2, 65536, 4, 10); }
  catch (const std::invalid_argument&) { threw = true; }
  assert(threw);
  threw = false;
  try { model.Penalty(0, 0, 0, 4, 10); }
  catch (const std::invalid_argument&) { threw = true; }
  assert(threw);
  threw = false;
  try { model.Observe(0, 0, 65536, 0, 10); }
  catch (const std::invalid_argument&) { threw = true; }
  assert(threw);
  std::cout << "conditional wait model tests passed\n";
}
