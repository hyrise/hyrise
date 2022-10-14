#include "operator_join_predicate.hpp"

#include "expression/abstract_predicate_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"

namespace hyrise {

std::optional<OperatorJoinPredicate> OperatorJoinPredicate::from_expression(const AbstractExpression& predicate,
                                                                            const AbstractLQPNode& left_input,
                                                                            const AbstractLQPNode& right_input) {
  const auto* abstract_predicate_expression = dynamic_cast<const AbstractPredicateExpression*>(&predicate);
  if (!abstract_predicate_expression) {
    return std::nullopt;
  }

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

  // It is possible that a join with left input A and right input B has a join predicate in the form of B.x = A.x. To
  // avoid having the join implementations handle such situations we check if the predicate sides match. If not, the
  // column IDs and the predicates are flipped.
  const auto find_predicate_in_input = [&](const auto& input) {
    std::optional<ColumnID> left_column_id{};
    std::optional<ColumnID> right_column_id{};

    input.iterate_output_expressions([&](const auto column_id, const auto& expression) {
      if (*expression == *abstract_predicate_expression->arguments[0]) {
        left_column_id = column_id;
      } else if (*expression == *abstract_predicate_expression->arguments[1]) {
        right_column_id = column_id;
      }
      return AbstractLQPNode::ExpressionIteration::Continue;
    });
    return std::make_pair(left_column_id, right_column_id);
  };

  const auto [left_in_left, right_in_left] = find_predicate_in_input(left_input);
  const auto [left_in_right, right_in_right] = find_predicate_in_input(right_input);

  const auto predicate_condition = abstract_predicate_expression->predicate_condition;
  if (left_in_left && right_in_right) {
    return OperatorJoinPredicate{{*left_in_left, *right_in_right}, predicate_condition};
  }

  if (right_in_left && left_in_right) {
    // Use the left column found in the right table and vice versa. Flip after construction to avoid code duplication.
    auto join_predicate = OperatorJoinPredicate{{*left_in_right, *right_in_left}, predicate_condition};
    join_predicate.flip();
    return join_predicate;
  }
  return std::nullopt;
}

OperatorJoinPredicate::OperatorJoinPredicate(const ColumnIDPair& init_column_ids,
                                             const PredicateCondition init_predicate_condition)
    : column_ids(init_column_ids), predicate_condition(init_predicate_condition) {}

void OperatorJoinPredicate::flip() {
  std::swap(column_ids.first, column_ids.second);
  predicate_condition = flip_predicate_condition(predicate_condition);
  flipped = !flipped;
}

bool OperatorJoinPredicate::is_flipped() const {
  return flipped;
}

bool operator<(const OperatorJoinPredicate& lhs, const OperatorJoinPredicate& rhs) {
  return std::tie(lhs.column_ids, lhs.predicate_condition) < std::tie(rhs.column_ids, rhs.predicate_condition);
}

bool operator==(const OperatorJoinPredicate& lhs, const OperatorJoinPredicate& rhs) {
  return std::tie(lhs.column_ids, lhs.predicate_condition) == std::tie(rhs.column_ids, rhs.predicate_condition);
}

}  // namespace hyrise
