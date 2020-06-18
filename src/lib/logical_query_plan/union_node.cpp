#include "union_node.hpp"

#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include "constant_mappings.hpp"
#include "expression/expression_utils.hpp"
#include "utils/assert.hpp"

namespace opossum {

UnionNode::UnionNode(const SetOperationMode init_set_operation_mode)
    : AbstractLQPNode(LQPNodeType::Union), set_operation_mode(init_set_operation_mode) {}

std::string UnionNode::description(const DescriptionMode mode) const {
  return "[UnionNode] Mode: " + set_operation_mode_to_string.left.at(set_operation_mode);
}

std::vector<std::shared_ptr<AbstractExpression>> UnionNode::output_expressions() const {
  Assert(expressions_equal(left_input()->output_expressions(), right_input()->output_expressions()),
         "Input Expressions must match");
  return left_input()->output_expressions();
}

std::vector<FunctionalDependency> UnionNode::functional_dependencies() const {
  switch (set_operation_mode) {
    case SetOperationMode::Unique:
    case SetOperationMode::All:
      Fail("Handling of functional dependencies is not yet specified for UNION (ALL)");
    case SetOperationMode::Positions:
      // By definition, UnionPositions requires both input tables to have the same table origin and structure.
      // Therefore, we can forward the FDs of either the left or right input node.
      DebugAssert(left_input()->functional_dependencies() == right_input()->functional_dependencies(),
                  "Expected both input nodes to have the same FDs.");
      return left_input()->functional_dependencies();
  }
  Fail("Unhandled UnionMode");
}

bool UnionNode::is_column_nullable(const ColumnID column_id) const {
  Assert(left_input() && right_input(), "Need both inputs to determine nullability");

  return left_input()->is_column_nullable(column_id) || right_input()->is_column_nullable(column_id);
}

size_t UnionNode::_on_shallow_hash() const { return boost::hash_value(set_operation_mode); }

std::shared_ptr<AbstractLQPNode> UnionNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return UnionNode::make(set_operation_mode);
}

bool UnionNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& union_node = static_cast<const UnionNode&>(rhs);
  return set_operation_mode == union_node.set_operation_mode;
}

}  // namespace opossum
