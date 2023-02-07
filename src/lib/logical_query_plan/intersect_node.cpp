#include "intersect_node.hpp"

#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include "constant_mappings.hpp"
#include "expression/expression_utils.hpp"
#include "utils/assert.hpp"

namespace hyrise {

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

UniqueColumnCombinations IntersectNode::unique_column_combinations() const {
  /**
   * Because INTERSECT acts as a pure filter for both input tables, all unique column combinations remain valid.
   *
   * Future Work: Merge unique column combinations from the left and right input node.
   */
  DebugAssert(left_input()->unique_column_combinations() == right_input()->unique_column_combinations(),
              "Merging of unique column combinations should be implemented.");
  return _forward_left_unique_column_combinations();
}

OrderDependencies IntersectNode::order_dependencies() const {
  return _forward_left_order_dependencies();
}

std::shared_ptr<InclusionDependencies> IntersectNode::inclusion_dependencies() const {
  return std::make_shared<InclusionDependencies>();
}

FunctionalDependencies IntersectNode::non_trivial_functional_dependencies() const {
  Fail("Merging of FDs is not implemented.");
}

size_t IntersectNode::_on_shallow_hash() const {
  return boost::hash_value(set_operation_mode);
}

std::shared_ptr<AbstractLQPNode> IntersectNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return IntersectNode::make(set_operation_mode);
}

bool IntersectNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& intersect_node = static_cast<const IntersectNode&>(rhs);
  return set_operation_mode == intersect_node.set_operation_mode;
}

}  // namespace hyrise
