#include "sort_node.hpp"

#include <sstream>
#include <string>
#include <vector>

#include "expression/expression_utils.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

SortNode::SortNode(const std::vector<std::shared_ptr<AbstractExpression>>& expressions,
                   const std::vector<SortMode>& init_sort_modes)
    : AbstractLQPNode(LQPNodeType::Sort, expressions), sort_modes(init_sort_modes) {
  Assert(expressions.size() == sort_modes.size(), "Expected as many Expressions as SortModes");
}

std::string SortNode::description(const DescriptionMode mode) const {
  const auto expression_mode = _expression_description_mode(mode);

  std::stringstream stream;

  stream << "[Sort] ";

  const auto node_expression_count = node_expressions.size();
  for (auto expression_idx = ColumnID{0}; expression_idx < node_expression_count; ++expression_idx) {
    stream << node_expressions[expression_idx]->description(expression_mode) << " ";
    stream << "(" << sort_modes[expression_idx] << ")";

    if (expression_idx + 1u < node_expression_count) {
      stream << ", ";
    }
  }
  return stream.str();
}

UniqueColumnCombinations SortNode::unique_column_combinations() const {
  return _forward_left_unique_column_combinations();
}

size_t SortNode::_on_shallow_hash() const {
  auto hash = size_t{0};
  for (const auto& sort_mode : sort_modes) {
    boost::hash_combine(hash, sort_mode);
  }
  return hash;
}

std::shared_ptr<AbstractLQPNode> SortNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return SortNode::make(expressions_copy_and_adapt_to_different_lqp(node_expressions, node_mapping), sort_modes);
}

bool SortNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& sort_node = static_cast<const SortNode&>(rhs);

  return expressions_equal_to_expressions_in_different_lqp(node_expressions, sort_node.node_expressions,
                                                           node_mapping) &&
         sort_modes == sort_node.sort_modes;
}

}  // namespace hyrise
