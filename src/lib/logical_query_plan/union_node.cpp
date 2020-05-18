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

std::vector<std::shared_ptr<AbstractExpression>> UnionNode::column_expressions() const {
  Assert(expressions_equal(left_input()->column_expressions(), right_input()->column_expressions()),
         "Input Expressions must match");
  return left_input()->column_expressions();
}

const std::shared_ptr<ExpressionsConstraintDefinitions> UnionNode::constraints() const {
  switch (union_mode) {
    case UnionMode::Positions:
      // UnionPositions merges two reference tables with the same original table(s). Any duplicate RowIDs are
      // filtered out. As a consequence, existing unique constraints from input tables can be forwarded.
      Assert(*left_input()->constraints() == *right_input()->constraints(),
             "Input tables should have the same constraints.");
      return forward_constraints();

    case UnionMode::All:
      // With UnionAll two tables of the same schema become merged. The resulting table might contain duplicates.
      // To forward constraints from previous nodes, we would have to ensure that both input tables are completely
      // distinct in terms of rows. Currently, there is no strategy. Therefore, we discard constraints from previous
      // nodes.
      return std::make_shared<ExpressionsConstraintDefinitions>();
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
