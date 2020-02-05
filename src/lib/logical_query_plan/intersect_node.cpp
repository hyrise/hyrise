#include "intersect_node.hpp"

#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include "constant_mappings.hpp"
#include "expression/expression_utils.hpp"
#include "utils/assert.hpp"

namespace opossum {

IntersectNode::IntersectNode(const UnionMode union_mode, const std::vector<std::shared_ptr<AbstractExpression>>& join_predicates) : AbstractLQPNode(LQPNodeType::Intersect, join_predicates), union_mode(union_mode) {}

std::string IntersectNode::description(const DescriptionMode mode) const {
  return "[IntersectNode] Mode: " + union_mode_to_string.left.at(union_mode);
}

const std::vector<std::shared_ptr<AbstractExpression>>& IntersectNode::column_expressions() const {
  return left_input()->column_expressions();
}

bool IntersectNode::is_column_nullable(const ColumnID column_id) const {
  Assert(left_input() && right_input(), "Need both inputs to determine nullability");

  return left_input()->is_column_nullable(column_id) || right_input()->is_column_nullable(column_id);
}

const std::vector<std::shared_ptr<AbstractExpression>>& IntersectNode::join_predicates() const {
  return node_expressions;
}

size_t IntersectNode::_on_shallow_hash() const { return boost::hash_value(union_mode); }

std::shared_ptr<AbstractLQPNode> IntersectNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return IntersectNode::make(union_mode, node_expressions);
}

bool IntersectNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& intersect_node = static_cast<const IntersectNode&>(rhs);
  return union_mode == intersect_node.union_mode;
}

}  // namespace opossum
