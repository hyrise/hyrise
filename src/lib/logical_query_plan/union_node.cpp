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

std::string UnionNode::description(const DescriptionMode mode) const {
  return "[UnionNode] Mode: " + union_mode_to_string.left.at(union_mode);
}

const std::vector<std::shared_ptr<AbstractExpression>>& UnionNode::column_expressions() const {
  Assert(expressions_equal(left_input()->column_expressions(), right_input()->column_expressions()),
         "Input Expressions must match");
  return left_input()->column_expressions();
}

bool UnionNode::is_column_nullable(const ColumnID column_id) const {
  Assert(left_input() && right_input(), "Need both inputs to determine nullability");

  return left_input()->is_column_nullable(column_id) || right_input()->is_column_nullable(column_id);
}

size_t UnionNode::_on_shallow_hash() const { return boost::hash_value(union_mode); }

std::shared_ptr<AbstractLQPNode> UnionNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return UnionNode::make(union_mode);
}

bool UnionNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& union_node = static_cast<const UnionNode&>(rhs);
  return union_mode == union_node.union_mode;
}

}  // namespace opossum
