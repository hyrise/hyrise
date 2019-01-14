#include "logical_plan_root_node.hpp" // NEEDEDINCLUDE

namespace opossum {

LogicalPlanRootNode::LogicalPlanRootNode() : AbstractLQPNode(LQPNodeType::Root) {}

std::string LogicalPlanRootNode::description() const { return "[LogicalPlanRootNode]"; }

std::shared_ptr<AbstractLQPNode> LogicalPlanRootNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return make();
}

bool LogicalPlanRootNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  return true;
}

}  // namespace opossum
