#include "union_node.hpp"

#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include "constant_mappings.hpp"
#include "expression/expression_utils.hpp"
#include "utils/assert.hpp"

namespace opossum {

UnionNode::UnionNode(const UnionMode union_mode) : AbstractLQPNode(LQPNodeType::Union), union_mode(union_mode) {}

std::string UnionNode::description() const { return "[UnionNode] Mode: " + union_mode_to_string.at(union_mode); }

const std::vector<std::shared_ptr<AbstractExpression>>& UnionNode::column_expressions() const {
  Assert(expressions_equal(left_input()->column_expressions(), right_input()->column_expressions()),
         "Input Expressions must match");
  return left_input()->column_expressions();
}

bool UnionNode::is_column_nullable(const ColumnID column_id) const {
  Assert(left_input() && right_input(), "Need both inputs to determine nullability");

  const auto left_input_column_count = left_input()->column_expressions().size();
  if (static_cast<size_t>(column_id) >= left_input_column_count) {
    return left_input()->is_column_nullable(column_id);
  } else {
    const auto right_column_id =
        static_cast<ColumnID>(column_id - static_cast<ColumnID::base_type>(left_input_column_count));
    return left_input()->is_column_nullable(right_column_id);
  }
}

std::shared_ptr<TableStatistics> UnionNode::derive_statistics_from(
    const std::shared_ptr<AbstractLQPNode>& left_input, const std::shared_ptr<AbstractLQPNode>& right_input) const {
  Fail("Statistics for UNION not yet implemented");
}

std::shared_ptr<AbstractLQPNode> UnionNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return UnionNode::make(union_mode);
}

bool UnionNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& union_node = static_cast<const UnionNode&>(rhs);
  return union_mode == union_node.union_mode;
}

}  // namespace opossum
