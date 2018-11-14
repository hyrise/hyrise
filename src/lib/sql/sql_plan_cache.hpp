#pragma once

#include <memory>
#include <string>

#include "cache/hash_cache.hpp"

namespace opossum {

class AbstractOperator;
class AbstractLQPNode;

using SQLPhysicalPlanCache = HashCache<std::shared_ptr<AbstractOperator>, std::string>;
using SQLLogicalPlanCache = HashCache<std::shared_ptr<AbstractLQPNode>, std::string>;

}  // namespace opossum
