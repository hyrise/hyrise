#pragma once

#include <optional>

#include "all_parameter_variant.hpp"
#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

class AbstractExpression;
class AbstractLPQNode;

// Predicate in a representation so that scan operators (TableScan, IndexScan) can use is. That is, it only
// consists of columns, values, a predicate condition and no nesting.
struct OperatorScanPredicate {
  /**
   * Try to build a conjunction of OperatorScanPredicates from an @param expression executed on @param node.
   * This *can* returns multiple as to allow for BETWEEN being split into two simple comparisons
   *
   * @return std::nullopt if that fails (e.g. the expression is a more complex expression)
   */
  static std::optional<std::vector<OperatorScanPredicate>> from_expression(const AbstractExpression& expression,
                                                                           const AbstractLQPNode& node);

  OperatorScanPredicate() = default;
  OperatorScanPredicate(const CxlumnID cxlumn_id, const PredicateCondition predicate_condition,
                        const AllParameterVariant& value = NullValue{});

  CxlumnID cxlumn_id{INVALID_cxlumn_id};
  PredicateCondition predicate_condition{PredicateCondition::Equals};
  AllParameterVariant value;
};

}  // namespace opossum
