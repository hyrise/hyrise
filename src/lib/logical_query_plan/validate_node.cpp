#include "validate_node.hpp"

#include <memory>
#include <string>

#include "logical_query_plan/abstract_lqp_node.hpp"

namespace hyrise {

ValidateNode::ValidateNode() : AbstractLQPNode(LQPNodeType::Validate) {}

std::string ValidateNode::description(const DescriptionMode /*mode*/) const {
  return "[Validate]";
}

UniqueColumnCombinations ValidateNode::unique_column_combinations() const {
  return _forward_left_unique_column_combinations();
}

std::shared_ptr<AbstractLQPNode> ValidateNode::_on_shallow_copy(LQPNodeMapping& /*node_mapping*/) const {
  return ValidateNode::make();
}

bool ValidateNode::_on_shallow_equals(const AbstractLQPNode& /*rhs*/, const LQPNodeMapping& /*node_mapping*/) const {
  return true;
}

}  // namespace hyrise
