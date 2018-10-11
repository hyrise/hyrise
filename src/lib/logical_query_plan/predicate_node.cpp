#include "predicate_node.hpp"

#include <memory>
#include <optional>
#include <sstream>
#include <string>

#include "constant_mappings.hpp"
#include "expression/between_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/parameter_expression.hpp"
#include "expression/value_expression.hpp"
#include "operators/operator_scan_predicate.hpp"
#include "statistics/table_statistics.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

PredicateNode::PredicateNode(const std::shared_ptr<AbstractExpression>& predicate)
    : AbstractLQPNode(LQPNodeType::Predicate), predicate(predicate) {}

std::string PredicateNode::description() const {
  std::stringstream stream;
  stream << "[Predicate] " << predicate->as_column_name();
  return stream.str();
}

std::vector<std::shared_ptr<AbstractExpression>> PredicateNode::node_expressions() const { return {predicate}; }

std::shared_ptr<TableStatistics> PredicateNode::derive_statistics_from(
    const std::shared_ptr<AbstractLQPNode>& left_input, const std::shared_ptr<AbstractLQPNode>& right_input) const {
  DebugAssert(left_input && !right_input, "PredicateNode need left_input and no right_input");

  /**
   * If the predicate is not a simple `<column> <predicate_condition> <value/column/placeholder>` predicate, 
   * then we have to fall back to a selectivity of 1 atm, because computing statistics for complex predicates
   * is not implemented.
   * Currently, we cannot compute statistics for, e.g., IN or nestings of AND/OR.
   */

  const auto operator_predicates = OperatorScanPredicate::from_expression(*predicate, *left_input);
  if (!operator_predicates) return left_input->get_statistics();

  auto output_statistics = left_input->get_statistics();

  for (const auto& operator_predicate : *operator_predicates) {
    output_statistics = std::make_shared<TableStatistics>(
        output_statistics->estimate_predicate(operator_predicate.column_id, operator_predicate.predicate_condition,
                                              operator_predicate.value, operator_predicate.value2));
  }

  return output_statistics;
}

std::shared_ptr<AbstractLQPNode> PredicateNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return std::make_shared<PredicateNode>(expression_copy_and_adapt_to_different_lqp(*predicate, node_mapping));
}

bool PredicateNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& predicate_node = static_cast<const PredicateNode&>(rhs);
  const auto equal =
      expression_equal_to_expression_in_different_lqp(*predicate, *predicate_node.predicate, node_mapping);

  return equal;
}

}  // namespace opossum
