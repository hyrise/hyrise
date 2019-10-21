#pragma once

#include <algorithm>
#include <array>
#include <map>
#include <memory>
#include <mutex>
#include <numeric>
#include <sstream>
#include <string>
#include <unordered_map>

#include "types.hpp"
#include "storage/base_segment.hpp"
#include "storage/table.hpp"

namespace opossum {

class SegmentAccessCounter : Noncopyable {
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

  struct AccessStatistics {
    std::array<uint64_t, AccessType::Count> count;
    std::string to_string() const {
      std::string str;
      str.reserve(AccessType::Count * 4);
      str.append(std::to_string(count[0]));
      for (auto it = count.cbegin() + 1, end_it = count.cend() - 1; it < end_it; ++it) {
        str.append(",");
        str.append(std::to_string(*it));
      }
      return str;
    }
  };

  const static AccessStatistics no_statistics;

  static SegmentAccessCounter& instance();
  void increase(const uint32_t segment_id,  AccessType type);
  void save_to_csv(const std::map<std::string, std::shared_ptr<Table>>& tables, const std::string& path) const;
  const AccessStatistics& statistics(const uint32_t segment_id) const;

 protected:
  SegmentAccessCounter() = default;
  ~SegmentAccessCounter() = default;
  SegmentAccessCounter(const SegmentAccessCounter&) = delete;
  const SegmentAccessCounter& operator=(const SegmentAccessCounter&) = delete;
// TODO: change type og segment_id
  std::mutex _statistics_lock;
  std::unordered_map<uint64_t, AccessStatistics> _statistics;
};

}  // namespace opossum
