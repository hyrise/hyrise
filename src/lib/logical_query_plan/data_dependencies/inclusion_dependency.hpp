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
 * Container structure to define inclusion dependencies (INDs) for LQP nodes. An inclusion dependency states that all
 * values of a table's column (or value combinations of multiple columns) are also present in another column(s), usually
 * in another table. In a database, an IND is always valid for foreign key relationships.
 *
 * There are different terminologies (primary/foreign key, referenced/referencing, including/included side). We decided
 * for "included" and "referenced" side. In the following example, nation.n_regionkey is the included column that
 * references region.r_regionkey. Translated from foreign key relationships (see ForeignKeyConstraint), the primary key
 * becomes the IND's referenced side, and the foreign key becomes the included side.
 *
 *    n_nationkey |    n_name | n_regionkey                r_regionkey |      r_name
 *   -------------+-----------+-------------              -------------+-------------
 *              0 |   ALGERIA |           1                          0 |      AFRICA
 *              1 | ARGENTINA |           1                          1 |     AMERICA
 *              2 |    BRAZIL |           1                          2 |        ASIA
 *              3 |    CANADA |           1                          3 |      EUROPE
 *              4 |     EGYPT |           4                          4 | MIDDLE EAST
 *              5 |  ETHIOPIA |           0
 *              6 |    FRANCE |           3
 *              7 |   GERMANY |           3
 *              8 |     INDIA |           2
 *              9 | INDONESIA |           2
 *             10 |      IRAN |           4
 *
 * We propagate INDs in LQPs for the referenced (primary key) side. Because an IND is (probably) ivalid whenever we
 * remove a single tuple from the referenced side, we can thus reduce the recursion depth when fetching INDs because
 * any LQP node that does not return all input tuples (PredicateNode, JoinNode, ...) can simply return an empty set of
 * INDs in most cases.
 *
 * When retrieving an IND from a ForeignKeyConstraint, we do not know the StoredTableNode (and, thus, expressions) of
 * the included side. For that reason, we store the table and ColumnIDs of the included side. If we want to check if an
 * IND holds for a given node, we can then check if (i) the IND is propagated and (ii) all (LQP column) expressions for
 * the included side are present. An IND for the example above looks like this:
 *     InclusionDependency{{nation->get_column("n_regionkey")}, {region->get_column_id_by_name("r_regionkey")}, region};
 *
 * NOTE: Because INDs are derived from soft constraints, which are not verified to be valid (especially for data
 *       changes), we cannot really safely assume that INDs are valid. Handling that is future work.
 */
struct InclusionDependency final {
  explicit InclusionDependency(std::vector<std::shared_ptr<AbstractExpression>>&& init_referenced_expressions,
                               std::vector<ColumnID>&& init_included_column_ids,
                               const std::shared_ptr<Table>& init_included_table);

  bool operator==(const InclusionDependency& rhs) const;
  bool operator!=(const InclusionDependency& rhs) const;
  size_t hash() const;

  std::vector<std::shared_ptr<AbstractExpression>> referenced_expressions;
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
