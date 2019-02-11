#pragma once

#include <memory>
#include <unordered_map>

#include "cost.hpp"

namespace opossum {

class AbstractLQPNode;

using CostEstimationCache = std::unordered_map<std::shared_ptr<AbstractLQPNode>, Cost>;

}  // namespace opossum
