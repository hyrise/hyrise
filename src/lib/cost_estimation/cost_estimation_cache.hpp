#pragma once

#include <memory>
#include <unordered_map>

#include "types.hpp"

namespace opossum {

class AbstractLQPNode;

struct CostEstimationCache {
  /**
   * Optional, can be enabled if the optimizer rule builds plans bottom-up
   */
  std::optional<std::unordered_map<std::shared_ptr<AbstractLQPNode>, Cost>> cost_by_lqp;
};

}  // namespace opossum
