#pragma once

#include <optional>

#include "all_parameter_variant.hpp"
#include "all_type_variant.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

class AbstractExpression;
class AbstractLPQNode;

// Predicate in a representation so that scan operators (TableScan, IndexScan) can use is. That is, it only
// consists of columns, values, a predicate condition and no nesting.
struct OperatorScanPredicate {
  /**
   * Try to build a conjunction of OperatorScanPredicates from an @param expression executed on @param node.
   * This *can* return multiple as to allow for BETWEEN being split into two simple comparisons
   *
   * @return std::nullopt if that fails (e.g. the expression is a more complex expression)
   */
  static std::optional<std::vector<OperatorScanPredicate>> from_expression(const AbstractExpression& expression,
                                                                           const AbstractLQPNode& node);

  OperatorScanPredicate() = default;
  OperatorScanPredicate(const ColumnID column_id, const PredicateCondition predicate_condition,
                        const AllParameterVariant& value = NullValue{},
                        const std::optional<AllParameterVariant>& value2 = std::nullopt);

  // Returns a string representation of the predicate, using an optionally given table that is used to resolve column
  // ids to names.
  std::ostream& output_to_stream(std::ostream& stream, const std::shared_ptr<const Table>& table = nullptr) const;

  ColumnID column_id{INVALID_COLUMN_ID};
  PredicateCondition predicate_condition{PredicateCondition::Equals};
  AllParameterVariant value;
  std::optional<AllParameterVariant> value2;
};

// For gtest
bool operator==(const OperatorScanPredicate& lhs, const OperatorScanPredicate& rhs);
std::ostream& operator<<(std::ostream& stream, const OperatorScanPredicate& predicate);

}  // namespace opossum
