#include "logical_plan_root_node.hpp"

#include <string>

#include "abstract_lqp_node.hpp"

namespace opossum {

LogicalPlanRootNode::LogicalPlanRootNode() : AbstractLQPNode(LQPNodeType::Root) {}

//std::string LogicalPlanRootNode::description() const { return "[LogicalPlanRootNode]"; }

std::shared_ptr<AbstractLQPNode> LogicalPlanRootNode::_shallow_copy_impl(LQPNodeMapping & node_mapping) const {
  return make();
}

bool LogicalPlanRootNode::_shallow_equals_impl(const AbstractLQPNode& rhs, const LQPNodeMapping & node_mapping) const {
  return true;
}

}  // namespace opossum
