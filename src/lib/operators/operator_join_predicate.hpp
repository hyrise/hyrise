#pragma once

#include <memory>
#include <optional>

#include "types.hpp"

namespace opossum {

class AbstractExpression;
class AbstractLQPNode;

// Predicate representation for Join operators consists of one cxlumn of each input side and a join predicate.
struct OperatorJoinPredicate {
  /**
   * Try to build an OperatorJoinPredicate from an @param expression executed on @param left_input and
   * @param right_input.
   * @return std::nullopt if that fails (e.g. the expression is a more complex expression)
   */
  static std::optional<OperatorJoinPredicate> from_expression(const AbstractExpression& predicate,
                                                              const AbstractLQPNode& left_input,
                                                              const AbstractLQPNode& right_input);

  OperatorJoinPredicate(const CxlumnIDPair& cxlumn_ids, const PredicateCondition predicate_condition);

  // `.first` is the Cxlumn in the left input, `.second` is the cxlumn in the right input
  CxlumnIDPair cxlumn_ids;
  PredicateCondition predicate_condition;
};

}  // namespace opossum
