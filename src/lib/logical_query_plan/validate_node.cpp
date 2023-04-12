#include "validate_node.hpp"

#include <string>

namespace hyrise {

ValidateNode::ValidateNode() : AbstractLQPNode(LQPNodeType::Validate) {}

std::string ValidateNode::description(const DescriptionMode /*mode*/) const {
  return "[Validate]";
}

UniqueColumnCombinations ValidateNode::unique_column_combinations() const {
  return _forward_left_unique_column_combinations();
}

OrderDependencies ValidateNode::order_dependencies() const {
  return _forward_left_order_dependencies();
}

InclusionDependencies ValidateNode::inclusion_dependencies() const {
  return _forward_left_inclusion_dependencies();
}

std::shared_ptr<AbstractLQPNode> ValidateNode::_on_shallow_copy(LQPNodeMapping& /*node_mapping*/) const {
  return ValidateNode::make();
}

bool ValidateNode::_on_shallow_equals(const AbstractLQPNode& /*rhs*/, const LQPNodeMapping& /*node_mapping*/) const {
  return true;
}

}  // namespace hyrise
