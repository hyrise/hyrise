#pragma once

#include <memory>
#include <string>

#include "cache/cache.hpp"

namespace opossum {

class AbstractOperator;
class AbstractLQPNode;

using SQLPhysicalPlanCache = Cache<std::shared_ptr<AbstractOperator>, std::string>;
using SQLLogicalPlanCache = Cache<std::shared_ptr<AbstractLQPNode>, std::string>;

}  // namespace opossum
