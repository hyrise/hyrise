#pragma once

#include <algorithm>
#include <array>
#include <map>
#include <memory>
#include <string>

#include "types.hpp"
#include "storage/table.hpp"

namespace opossum {

class SegmentAccessCounter : Noncopyable {
 public:
  static void save_to_csv(const std::map<std::string, std::shared_ptr<Table>>& tables, const std::string& path);
  static void reset(const std::map<std::string, std::shared_ptr<Table>>& tables);

 protected:
  SegmentAccessCounter() = default;
  ~SegmentAccessCounter() = default;
  SegmentAccessCounter(const SegmentAccessCounter&) = delete;
  const SegmentAccessCounter& operator=(const SegmentAccessCounter&) = delete;
};

}  // namespace opossum
