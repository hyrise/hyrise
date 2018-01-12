#include "logical_plan_root_node.hpp"

#include <string>

#include "abstract_lqp_node.hpp"

namespace opossum {

LogicalPlanRootNode::LogicalPlanRootNode() : AbstractLQPNode(LQPNodeType::Root) {}

std::shared_ptr<AbstractLQPNode> LogicalPlanRootNode::_deep_copy_impl(
    const std::shared_ptr<AbstractLQPNode>& copied_left_child,
    const std::shared_ptr<AbstractLQPNode>& copied_right_child) const {
  return std::make_shared<LogicalPlanRootNode>();
}

std::string LogicalPlanRootNode::description() const { return "[LogicalPlanRootNode]"; }

}  // namespace opossum
