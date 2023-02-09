#pragma once

#include "expression/abstract_expression.hpp"
#include "types.hpp"

namespace hyrise {

class Table;

/**
 * TODO
 */
struct InclusionDependency final {
  explicit InclusionDependency(const std::vector<std::shared_ptr<AbstractExpression>>& init_expressions,
                               const std::vector<ColumnID>& init_included_column_ids,
                               const std::shared_ptr<Table>& init_included_table);

  bool operator==(const InclusionDependency& rhs) const;
  bool operator!=(const InclusionDependency& rhs) const;
  size_t hash() const;

  std::vector<std::shared_ptr<AbstractExpression>> expressions;
  std::vector<ColumnID> included_column_ids;
  std::shared_ptr<Table> included_table;
};

std::ostream& operator<<(std::ostream& stream, const InclusionDependency& ind);

using InclusionDependencies = std::unordered_set<InclusionDependency>;

}  // namespace hyrise

namespace std {

template <>
struct hash<hyrise::InclusionDependency> {
  size_t operator()(const hyrise::InclusionDependency& ind) const;
};

}  // namespace std
