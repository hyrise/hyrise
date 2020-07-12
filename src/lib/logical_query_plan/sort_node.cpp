#include "sort_node.hpp"

#include <sstream>
#include <string>
#include <vector>

#include "constant_mappings.hpp"
#include "expression/expression_utils.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

SortNode::SortNode(const std::vector<std::shared_ptr<AbstractExpression>>& expressions,
                   const std::vector<SortMode>& init_sort_modes)
    : AbstractLQPNode(LQPNodeType::Sort, expressions), sort_modes(init_sort_modes) {
  Assert(expressions.size() == sort_modes.size(), "Expected as many Expressions as SortModes");
}

std::string SortNode::description(const DescriptionMode mode) const {
  const auto expression_mode = _expression_description_mode(mode);

  std::stringstream stream;

  stream << "[Sort] ";

  for (auto expression_idx = size_t{0}; expression_idx < node_expressions.size(); ++expression_idx) {
    stream << node_expressions[expression_idx]->description(expression_mode) << " ";
    stream << "(" << sort_modes[expression_idx] << ")";

    if (expression_idx + 1 < node_expressions.size()) stream << ", ";
  }
  return stream.str();
}

std::shared_ptr<LQPUniqueConstraints> SortNode::unique_constraints() const {
  return _forward_left_unique_constraints();
}

size_t SortNode::_on_shallow_hash() const {
  size_t hash{0};
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

}  // namespace opossum
