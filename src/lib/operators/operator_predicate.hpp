#pragma once

#include <optional>

#include "types.hpp"
#include "all_type_variant.hpp"
#include "all_parameter_variant.hpp"

namespace opossum {

class AbstractExpression;
class AbstractLPQNode;

// Predicate in a representation so that operators (Joins, TableScan, IndexScan, ...) can use the. That is, it only
// consists of columns, values, a predicate condition and no nesting.
struct OperatorPredicate {
  /**
   * Try to build an OperatorPredicate from an @param expression executed on @param node.
   * @return std::nullopt if that fails (e.g. the expression is a more complex expression)
   */
  static std::optional<OperatorPredicate> from_expression(const AbstractExpression& expression, const AbstractLQPNode& node);

  OperatorPredicate() = default;
  OperatorPredicate(const ColumnID column_id,
                    const PredicateCondition predicate_condition,
                    const AllParameterVariant& value = NullValue{},
                    const std::optional<AllParameterVariant>& value2 = {});

  ColumnID column_id{INVALID_COLUMN_ID};
  PredicateCondition predicate_condition{PredicateCondition::Equals};
  AllParameterVariant value;
  std::optional<AllParameterVariant> value2;
};

}  // namespace opossum
