#pragma once

#include <unordered_set>

#include "expression/abstract_expression.hpp"
#include "types.hpp"

namespace hyrise {

/**
 * Container structure to define uniqueness for subsets of LQP output expressions. Analogous to SQL's UNIQUE
 * constraint, rows containing NULL values in any of the expressions are always considered to be distinct. For
 * PRIMARY KEY semantics, check if the expressions are nullable, cf. AbstractLQPNode::is_column_nullable.
 *
 * NOTE: Unique column combinations (UCCs) are only valid for LQP nodes that contain no invalidated rows (i.e., where
 *       there has been a ValidateNode before or where MVCC is disabled).
 *
 * If a UCC may become invalid in the future (because it is not based on a schema constraint, but on the data
 * incidentally being unique at the moment), the UCC is marked as being not schema-given.
 * This information is important because query plans that were optimized using a non-schema-given UCC are not safely
 * cacheable.
 */
struct UniqueColumnCombination final {
  explicit UniqueColumnCombination(ExpressionUnorderedSet&& init_expressions, bool is_schema_given = true);

  bool operator==(const UniqueColumnCombination& rhs) const;
  bool operator!=(const UniqueColumnCombination& rhs) const;

  bool is_schema_given() const;
  void set_schema_given() const;
  size_t hash() const;

  ExpressionUnorderedSet expressions;

 private:
  mutable bool _is_schema_given;
};

std::ostream& operator<<(std::ostream& stream, const UniqueColumnCombination& ucc);

using UniqueColumnCombinations = std::unordered_set<UniqueColumnCombination>;

}  // namespace hyrise

namespace std {

template <>
struct hash<hyrise::UniqueColumnCombination> {
  size_t operator()(const hyrise::UniqueColumnCombination& ucc) const;
};

}  // namespace std
