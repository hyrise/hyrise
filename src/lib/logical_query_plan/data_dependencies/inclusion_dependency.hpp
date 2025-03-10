#pragma once

#include <memory>
#include <ostream>
#include <unordered_set>
#include <vector>

#include "expression/abstract_expression.hpp"
#include "types.hpp"

namespace hyrise {

class Table;

/**
 * Container structure to define inclusion dependencies (INDs) for LQP nodes. An inclusion dependency on a given LQP
 * node expresses that the resulting table's values for `expressions` (still) contain all values existing in the columns
 * specified by the `included_column_ids` of `included_table`. Inclusion dependencies can arise from foreign keys.
 *
 * Inclusion dependencies involve two relations. We only propagate them from the including side. For example, we
 * consider the IND nation.n_regionkey in region.r_regionkey using a StoredTableNode for nation and a table for region.
 * The IND of this looks like this:
 *     InclusionDependency{{nation->get_column("n_regionkey")}, {nation->get_column_id_by_name("r_regionkey")}, region}
 *
 * NOTE: Inclusion dependencies (INDs) are only valid for LQP nodes that contain no invalidated rows (i.e., where there
 *       has been a ValidateNode before or where MVCC is disabled).
 */
struct InclusionDependency final {
  explicit InclusionDependency(std::vector<std::shared_ptr<AbstractExpression>>&& init_expressions,
                               std::vector<ColumnID>&& init_included_column_ids,
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
