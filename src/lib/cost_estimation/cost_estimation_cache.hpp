#pragma once

#include <memory>
#include <unordered_map>

#include "types.hpp"

namespace opossum {

class AbstractLQPNode;

struct CostEstimationCache {
  /**
   * Optional, can be enabled if the optimizer rule builds plans bottom-up
   * At the moment our means of cost caching are limited to a <node-ptr> -> <cost> mapping
   */
  std::optional<std::unordered_map<std::shared_ptr<AbstractLQPNode>, Cost>> cost_by_lqp;
};

}  // namespace opossum
