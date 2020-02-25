#include "except_node.hpp"

#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include "constant_mappings.hpp"
#include "expression/expression_utils.hpp"
#include "utils/assert.hpp"

namespace opossum {

ExceptNode::ExceptNode(const UnionMode union_mode,
                       const std::vector<std::shared_ptr<AbstractExpression>>& join_predicates)
    : AbstractLQPNode(LQPNodeType::Except, join_predicates), union_mode(union_mode) {}

std::string ExceptNode::description(const DescriptionMode mode) const {
  return "[ExceptNode] Mode: " + union_mode_to_string.left.at(union_mode);
}

const std::vector<std::shared_ptr<AbstractExpression>>& ExceptNode::column_expressions() const {
  return left_input()->column_expressions();
}

bool ExceptNode::is_column_nullable(const ColumnID column_id) const {
  Assert(left_input() && right_input(), "Need both inputs to determine nullability");

  return left_input()->is_column_nullable(column_id) || right_input()->is_column_nullable(column_id);
}

const std::vector<std::shared_ptr<AbstractExpression>>& ExceptNode::join_predicates() const { return node_expressions; }

size_t ExceptNode::_on_shallow_hash() const { return boost::hash_value(union_mode); }

std::shared_ptr<AbstractLQPNode> ExceptNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return ExceptNode::make(union_mode, node_expressions);
}

bool ExceptNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& except_node = static_cast<const ExceptNode&>(rhs);
  return union_mode == except_node.union_mode;
}

}  // namespace opossum
