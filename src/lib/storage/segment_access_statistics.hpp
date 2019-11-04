#pragma once

#include <array>
#include <atomic>
#include <string>

namespace opossum {

class SegmentAccessStatistics {
 public:
  enum AccessType {
    DirectRead,
    Append,
    Reserve,
    IteratorAccess,
    IteratorPointAccess,
    AccessorAccess,
    ReferenceAccessorAccess,
    Materialization,
    // number of elements in enum
      Count
  };

  SegmentAccessStatistics();

  void increase(AccessType type);
  void reset_all();
  uint64_t count(AccessType type);
  std::string to_string() const;

  const static SegmentAccessStatistics no_statistics;
  static bool use_locking;

 private:
  std::array<std::atomic_uint64_t, AccessType::Count> _atomic_count;
  std::array<uint64_t, AccessType::Count> _count;
};

}  // namespace opossum
