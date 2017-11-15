#include "logical_plan_root_node.hpp"

#include <string>

#include "abstract_logical_query_plan_node.hpp"

namespace opossum {

LogicalPlanRootNode::LogicalPlanRootNode() : AbstractLogicalQueryPlanNode(LQPNodeType::Root) {}

std::string LogicalPlanRootNode::description() const { return "[LogicalPlanRootNode]"; }

}  // namespace opossum
