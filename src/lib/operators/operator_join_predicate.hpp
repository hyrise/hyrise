#pragma once

#include <memory>
#include <optional>

#include "types.hpp"

namespace opossum {

class AbstractExpression;
class AbstractLQPNode;
class JoinNode;

// Predicate representation for Join operators consists of one column of each input side and a join predicate.
struct OperatorJoinPredicate {
  /**
   * Try to build an OperatorJoinPredicate from an @param expression executed on @param left_input and
   * @param right_input.
   * @return std::nullopt if that fails (e.g. the expression is a more complex expression)
   */
  static std::optional<OperatorJoinPredicate> from_expression(const AbstractExpression& predicate,
                                                              const AbstractLQPNode& left_input,
                                                              const AbstractLQPNode& right_input);
  /**
   * Flip operands and PredicateCondition
   */
  void flip();

  OperatorJoinPredicate(const ColumnIDPair& init_column_ids, const PredicateCondition init_predicate_condition);

  // `.first` is the Column in the left input, `.second` is the column in the right input
  ColumnIDPair column_ids;
  PredicateCondition predicate_condition;
};

// For gtest
bool operator<(const OperatorJoinPredicate& l, const OperatorJoinPredicate& r);
bool operator==(const OperatorJoinPredicate& l, const OperatorJoinPredicate& r);

}  // namespace opossum
