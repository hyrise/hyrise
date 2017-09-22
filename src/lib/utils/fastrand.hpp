#pragma once

#include <cstdlib>

namespace opossum {

static unsigned int g_seed = std::rand();

// fastrand routine returns one integer, similar output value range as C lib.
inline int fastrand() {
  g_seed = (214013 * g_seed + 2531011);
  return (g_seed >> 16) & 0x7FFF;
}

}  // namespace opossum