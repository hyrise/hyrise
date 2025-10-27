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
 * NOTE: Because unique column combinations (UCCs) are derived from soft constraints, which are not verified to be valid
 *       (especially for data changes), we cannot really safely assume that UCCs are valid. Handling that is future
 *       work. For UCCs derived from discovered constraints, i.e., not defined by the DDL, we currently have means to
 *       guarantee correct query plans even for changing data.
 */
struct UniqueColumnCombination final {
  explicit UniqueColumnCombination(ExpressionUnorderedSet&& init_expressions);

  bool operator==(const UniqueColumnCombination& rhs) const;
  bool operator!=(const UniqueColumnCombination& rhs) const;
  size_t hash() const;

  ExpressionUnorderedSet expressions;
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
