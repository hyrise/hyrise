#include <cmath>

#include "utils/assert.hpp"

namespace opossum {

// Returns the result of a positive integer division rounded both to zero and to the nearest int
// e.g. div_both
template <typename A, typename B>
auto div_with_rounding(const A& a, const B& b) {
  DebugAssert(a >= 0 && b > 0, "Can only operate on positive integers");
  const auto round_to_zero = a / b;
  auto round_to_nearest = a / b;
  if (a - round_to_zero * b >= b / 2) round_to_nearest += 1;
  return std::make_pair(round_to_zero, round_to_nearest);
}

}  // namespace opossum
