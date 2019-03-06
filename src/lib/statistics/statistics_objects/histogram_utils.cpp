#include "histogram_utils.hpp"

#include <cmath>

#include <algorithm>
#include <limits>
#include <string>
#include <utility>

#include "utils/assert.hpp"

namespace opossum {

uint64_t ipow(uint64_t base, uint64_t exp) {
  // Taken from https://stackoverflow.com/a/101613/2362807.
  // Note: this function does not check for any possible overflows!
  uint64_t result = 1;

  for (;;) {
    if (exp & 1u) {
      result *= base;
    }

    exp >>= 1u;

    if (!exp) {
      break;
    }

    base *= base;
  }

  return result;
}

}  // namespace opossum
