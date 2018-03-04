#include "logical_plan_root_node.hpp"

#include <string>

#include "abstract_lqp_node.hpp"

namespace opossum {

LogicalPlanRootNode::LogicalPlanRootNode() : AbstractLQPNode(LQPNodeType::Root) {}

std::shared_ptr<AbstractLQPNode> LogicalPlanRootNode::_deep_copy_impl(
    const std::shared_ptr<AbstractLQPNode>& copied_left_input,
    const std::shared_ptr<AbstractLQPNode>& copied_right_input) const {
  return LogicalPlanRootNode::make();
}

std::string LogicalPlanRootNode::description() const { return "[LogicalPlanRootNode]"; }

bool LogicalPlanRootNode::shallow_equals(const AbstractLQPNode& rhs) const {
  Assert(rhs.type() == type(), "Can only compare nodes of the same type()");
  return true;
}

}  // namespace opossum
