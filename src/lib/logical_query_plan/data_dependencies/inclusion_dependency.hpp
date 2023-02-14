#pragma once

#include "expression/abstract_expression.hpp"
#include "types.hpp"

namespace hyrise {

class Table;

/**
 * Container structure to define inclusion dependencies for LQP nodes. An inclusion dependency that is valid on a given
 * LQP node expresses that the resulting table' values for `expressions` still contains all values existing in the
 * columns specified by the `included_column_ids` of `included_table`.
 *
 * For example, we consider the IND nation.n_regionkey in region.r_regionkey and a StoredTableNode node. The valid IND
 * of this looks like this: InclusionDependency{{node->get_column("n_regionkey")}, {ColumnID{0}}, region}
 *
 * NOTE: Inclusion dependencies (INDs) are only valid for LQP nodes that contain no invalidated rows (i.e., where there
 *       has been a ValidateNode before or where MVCC is disabled).
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
