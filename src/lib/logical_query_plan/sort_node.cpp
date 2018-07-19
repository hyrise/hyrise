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
    : AbstractLQPNode(LQPNodeType::Sort), expressions(expressions), order_by_modes(order_by_modes) {
  Assert(expressions.size() == order_by_modes.size(), "Expected as many Expressions as OrderByModes");
}

std::string SortNode::description() const {
  std::stringstream stream;

  stream << "[Sort] ";

  for (auto expression_idx = size_t{0}; expression_idx < expressions.size(); ++expression_idx) {
    stream << expressions[expression_idx]->as_column_name() << " ";
    stream << "(" << order_by_mode_to_string.at(order_by_modes[expression_idx]) << ")";

    if (expression_idx + 1 < expressions.size()) stream << ", ";
  }
  return stream.str();
}

std::vector<std::shared_ptr<AbstractExpression>> SortNode::node_expressions() const { return expressions; }

std::shared_ptr<AbstractLQPNode> SortNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return SortNode::make(expressions_copy_and_adapt_to_different_lqp(expressions, node_mapping), order_by_modes);
}

bool SortNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& sort_node = static_cast<const SortNode&>(rhs);

  return expressions_equal_to_expressions_in_different_lqp(expressions, sort_node.expressions, node_mapping) &&
         order_by_modes == sort_node.order_by_modes;
}

}  // namespace opossum
