#pragma once

#include <memory>
#include <string>

#include "cache/gdfs_cache.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "storage/prepared_plan.hpp"

namespace opossum {

class AbstractOperator;
class AbstractLQPNode;

using SQLPhysicalPlanCache = GDFSCache<std::string, std::shared_ptr<AbstractOperator>>;
using SQLLogicalPlanCache = GDFSCache<std::shared_ptr<AbstractLQPNode>, std::shared_ptr<PreparedPlan>,
                                      LQPNodeSharedPtrHash, LQPNodeSharedPtrEqual>;

}  // namespace opossum
