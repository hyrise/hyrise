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

std::vector<std::shared_ptr<AbstractExpression>> IntersectNode::output_expressions() const {
  return left_input()->output_expressions();
}

bool IntersectNode::is_column_nullable(const ColumnID column_id) const {
  Assert(left_input() && right_input(), "Need both inputs to determine nullability");

  return left_input()->is_column_nullable(column_id) || right_input()->is_column_nullable(column_id);
}

std::shared_ptr<LQPUniqueConstraints> IntersectNode::unique_constraints() const {
  /**
   * Because INTERSECT acts as a pure filter for both input tables, all unique constraints remain valid.
   *
   * Future Work: Merge unique constraints from the left and right input node.
   */
  DebugAssert(left_input()->unique_constraints() == right_input()->unique_constraints(),
              "Merging of unique constraints should be implemented.");
  return _forward_left_unique_constraints();
}

std::vector<FunctionalDependency> IntersectNode::non_trivial_functional_dependencies() const {
  Fail("Merging of FDs should be implemented.");
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
