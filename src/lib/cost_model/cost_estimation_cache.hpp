#pragma once

#include <memory>
#include <unordered_map>

#include "cost.hpp"

namespace opossum {

class AbstractLQPNode;

/**
 * For now, our means of cost caching are limited to a <node-ptr> -> <cost> mapping
 */
using CostEstimationCache = std::unordered_map<std::shared_ptr<AbstractLQPNode>, Cost>;

}  // namespace opossum
