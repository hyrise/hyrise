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
#include "operators/operator_predicate.hpp"
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
   * If the predicate is a not simple `<column> <predicate_condition> <value>` predicate, then we have to
   * fall back to a selectivity of 1 atm, because computing statistics for complex predicates is
   * not implemented
   */
  std::cout << "Predicate: " << predicate->as_column_name() << std::endl;
  std::cout << "Columns: " << expression_column_names(left_input->column_expressions()) << std::endl;

  left_input->print();

  const auto operator_predicate = OperatorPredicate::from_expression(*predicate, *left_input);
  if (!operator_predicate) return left_input->get_statistics();

  return std::make_shared<TableStatistics>(left_input->get_statistics()->estimate_predicate(
      operator_predicate->column_id,
      operator_predicate->predicate_condition,
      operator_predicate->value,
      operator_predicate->value2));
}

std::shared_ptr<AbstractLQPNode> PredicateNode::_shallow_copy_impl(LQPNodeMapping& node_mapping) const {
  return std::make_shared<PredicateNode>(expression_copy_and_adapt_to_different_lqp(*predicate, node_mapping));
}

bool PredicateNode::_shallow_equals_impl(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& predicate_node = static_cast<const PredicateNode&>(rhs);
  const auto equal =
      expression_equal_to_expression_in_different_lqp(*predicate, *predicate_node.predicate, node_mapping);

  return equal;
}

}  // namespace opossum
