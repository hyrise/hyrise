#pragma once

#include <memory>

#include "selectivity.hpp"

namespace opossum {

class BaseSegmentStatistics2 {
 public:
  virtual ~BaseSegmentStatistics2() = default;

  virtual std::shared_ptr<BaseSegmentStatistics2> scale_with_selectivity(const Selectivity selectivity) const = 0;
};

}  // namespace opossum
