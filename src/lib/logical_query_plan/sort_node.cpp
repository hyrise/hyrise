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
                   const std::vector<OrderByMode>& order_by_modes)
    : AbstractLQPNode(LQPNodeType::Sort, expressions), order_by_modes(order_by_modes) {
  Assert(expressions.size() == order_by_modes.size(), "Expected as many Expressions as OrderByModes");
}

std::string SortNode::description() const {
  std::stringstream stream;

  stream << "[Sort] ";

  for (auto expression_idx = size_t{0}; expression_idx < node_expressions.size(); ++expression_idx) {
    stream << node_expressions[expression_idx]->as_column_name() << " ";
    stream << "(" << order_by_modes[expression_idx] << ")";

    if (expression_idx + 1 < node_expressions.size()) stream << ", ";
  }
  return stream.str();
}

std::shared_ptr<AbstractLQPNode> SortNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return SortNode::make(expressions_copy_and_adapt_to_different_lqp(node_expressions, node_mapping), order_by_modes);
}

bool SortNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& sort_node = static_cast<const SortNode&>(rhs);

  return expressions_equal_to_expressions_in_different_lqp(node_expressions, sort_node.node_expressions,
                                                           node_mapping) &&
         order_by_modes == sort_node.order_by_modes;
}

}  // namespace opossum
