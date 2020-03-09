#include "operator_join_predicate.hpp"

#include "expression/abstract_predicate_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"

namespace opossum {

std::optional<OperatorJoinPredicate> OperatorJoinPredicate::from_expression(const AbstractExpression& predicate,
                                                                            const AbstractLQPNode& left_input,
                                                                            const AbstractLQPNode& right_input) {
  const auto* abstract_predicate_expression = dynamic_cast<const AbstractPredicateExpression*>(&predicate);
  if (!abstract_predicate_expression) return std::nullopt;

  switch (abstract_predicate_expression->predicate_condition) {
    case PredicateCondition::Equals:
    case PredicateCondition::NotEquals:
    case PredicateCondition::LessThan:
    case PredicateCondition::LessThanEquals:
    case PredicateCondition::GreaterThan:
    case PredicateCondition::GreaterThanEquals:
      break;
    default:
      return std::nullopt;
  }

  Assert(abstract_predicate_expression->arguments.size() == 2u, "Expected two arguments");

  const auto left_in_left = left_input.find_column_id(*abstract_predicate_expression->arguments[0]);
  const auto left_in_right = right_input.find_column_id(*abstract_predicate_expression->arguments[0]);
  const auto right_in_left = left_input.find_column_id(*abstract_predicate_expression->arguments[1]);
  const auto right_in_right = right_input.find_column_id(*abstract_predicate_expression->arguments[1]);

  auto predicate_condition = abstract_predicate_expression->predicate_condition;

  if (left_in_left && right_in_right) {
    return OperatorJoinPredicate{{*left_in_left, *right_in_right}, predicate_condition};
  }

  if (right_in_left && left_in_right) {
    predicate_condition = flip_predicate_condition(predicate_condition);
    return OperatorJoinPredicate{{*right_in_left, *left_in_right}, predicate_condition};
  }

  return std::nullopt;
}

OperatorJoinPredicate::OperatorJoinPredicate(const ColumnIDPair& init_column_ids,
                                             const PredicateCondition init_predicate_condition)
    : column_ids(init_column_ids), predicate_condition(init_predicate_condition) {}

void OperatorJoinPredicate::flip() {
  std::swap(column_ids.first, column_ids.second);
  predicate_condition = flip_predicate_condition(predicate_condition);
}

bool operator<(const OperatorJoinPredicate& l, const OperatorJoinPredicate& r) {
  return std::tie(l.column_ids, l.predicate_condition) < std::tie(r.column_ids, r.predicate_condition);
}

bool operator==(const OperatorJoinPredicate& l, const OperatorJoinPredicate& r) {
  return std::tie(l.column_ids, l.predicate_condition) == std::tie(r.column_ids, r.predicate_condition);
}

}  // namespace opossum
