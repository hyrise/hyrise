#include "intersect_node.hpp"

#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include "constant_mappings.hpp"
#include "expression/expression_utils.hpp"
#include "utils/assert.hpp"

namespace opossum {

IntersectNode::IntersectNode(const SetOperationMode init_operation_mode)
    : AbstractLQPNode(LQPNodeType::Intersect), set_operation_mode(init_operation_mode) {}

std::string IntersectNode::description(const DescriptionMode mode) const {
  return "[IntersectNode] Mode: " + set_operation_mode_to_string.left.at(set_operation_mode);
}

std::vector<std::shared_ptr<AbstractExpression>> IntersectNode::column_expressions() const {
  return input_left()->column_expressions();
}

bool IntersectNode::is_column_nullable(const ColumnID column_id) const {
  Assert(input_left() && input_right(), "Need both inputs to determine nullability");

  return input_left()->is_column_nullable(column_id) || input_right()->is_column_nullable(column_id);
}

size_t IntersectNode::_on_shallow_hash() const { return boost::hash_value(set_operation_mode); }

std::shared_ptr<AbstractLQPNode> IntersectNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return IntersectNode::make(set_operation_mode);
}

bool IntersectNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& intersect_node = static_cast<const IntersectNode&>(rhs);
  return set_operation_mode == intersect_node.set_operation_mode;
}

}  // namespace opossum
