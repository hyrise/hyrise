#pragma once

#include <cstdint>
#include <iostream>

namespace opossum {

class MemoryUsage final {
public:
  explicit MemoryUsage(const size_t bytes);

  void print(std::ostream& stream) const;

  size_t bytes;
};

// Data structures providing memory usage estimation can use this as a hint as to whether to be fast but imprecise or
// try to determine the memory usage as exactly as possible, but possibly taking more time to do so.
enum class MemoryUsageEstimationMode { Fast, MoreExact };

}  // namespace opossum
