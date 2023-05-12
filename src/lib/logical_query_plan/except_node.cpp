#include "except_node.hpp"

#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include "expression/expression_utils.hpp"
#include "utils/assert.hpp"

namespace hyrise {

ExceptNode::ExceptNode(const SetOperationMode init_operation_mode)
    : AbstractLQPNode(LQPNodeType::Except), set_operation_mode(init_operation_mode) {}

std::string ExceptNode::description(const DescriptionMode /*mode*/) const {
  return "[ExceptNode] Mode: " + std::string{magic_enum::enum_name(set_operation_mode)};
}

std::vector<std::shared_ptr<AbstractExpression>> ExceptNode::output_expressions() const {
  return left_input()->output_expressions();
}

bool ExceptNode::is_column_nullable(const ColumnID column_id) const {
  Assert(left_input() && right_input(), "Need both inputs to determine nullability");

  return left_input()->is_column_nullable(column_id) || right_input()->is_column_nullable(column_id);
}

UniqueColumnCombinations ExceptNode::unique_column_combinations() const {
  // Because EXCEPT acts as a pure filter for the left input table, all unique column combinations from the left input
  // node remain valid.
  return _forward_left_unique_column_combinations();
}

FunctionalDependencies ExceptNode::non_trivial_functional_dependencies() const {
  // The right input node is used for filtering only. It does not contribute any FDs.
  return left_input()->non_trivial_functional_dependencies();
}

size_t ExceptNode::_on_shallow_hash() const {
  return boost::hash_value(set_operation_mode);
}

std::shared_ptr<AbstractLQPNode> ExceptNode::_on_shallow_copy(LQPNodeMapping& /*node_mapping*/) const {
  return ExceptNode::make(set_operation_mode);
}

bool ExceptNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& /*node_mapping*/) const {
  const auto& except_node = static_cast<const ExceptNode&>(rhs);
  return set_operation_mode == except_node.set_operation_mode;
}

}  // namespace hyrise
