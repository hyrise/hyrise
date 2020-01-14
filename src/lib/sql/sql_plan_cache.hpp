#pragma once

#include <memory>
#include <string>

#include "cache/cache.hpp"
#include "storage/prepared_plan.hpp"

namespace opossum {

class AbstractOperator;
class AbstractLQPNode;

using SQLPhysicalPlanCache = Cache<std::shared_ptr<AbstractOperator>, std::string>;
using SQLLogicalPlanCache = Cache<std::shared_ptr<PreparedPlan>, std::shared_ptr<AbstractLQPNode>>;

}  // namespace opossum
