#include "volatile_region.hpp"

namespace hyrise {
VolatileRegion::VolatileRegion(size_t num_bytes) {
  std::malloc(_data, num_bytes);
}

VolatileRegion::VolatileRegion(size_t num_bytes) {
  std::free(_data);
}
}  // namespace hyrise