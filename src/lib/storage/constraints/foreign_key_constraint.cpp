#include "foreign_key_constraint.hpp"

#include <numeric>

#include <boost/container_hash/hash.hpp>

namespace {

using namespace hyrise;  // NOLINT(build/namespaces)

// ForeignKeyConstraints with swapped columns are equivalent. To ensure they are represented the same way, we determine
// the permutation that sorts `foreign_key_columns` and apply it to `foreign_key_columns` and `primary_key_columns`.
// Idea taken from https://stackoverflow.com/a/17074810
std::vector<size_t> sort_indexes(const std::vector<ColumnID>& column_ids) {
  auto permutation = std::vector<size_t>(column_ids.size());
  // Fill permutation with [0, 1, ..., n - 1] and order the permutation by sorting column_ids.
  std::iota(permutation.begin(), permutation.end(), 0);
  std::sort(permutation.begin(), permutation.end(),
            [&](auto lhs, auto rhs) { return column_ids[lhs] < column_ids[rhs]; });
  return permutation;
}

std::vector<ColumnID> apply_permutation(std::vector<ColumnID>& column_ids, const std::vector<size_t>& permutation) {
  auto sorted_column_ids = std::vector<ColumnID>(column_ids.size());

  std::transform(permutation.begin(), permutation.end(), sorted_column_ids.begin(),
                 [&](const auto position) { return column_ids[position]; });
  return sorted_column_ids;
}

}  // namespace

namespace hyrise {

ForeignKeyConstraint::ForeignKeyConstraint(const std::vector<ColumnID>& foreign_key_columns,
                                           const std::shared_ptr<Table>& foreign_key_table,
                                           const std::vector<ColumnID>& primary_key_columns,
                                           const std::shared_ptr<Table>& primary_key_table)
    : AbstractTableConstraint(TableConstraintType::ForeignKey),
      _foreign_key_columns{foreign_key_columns},
      _foreign_key_table{foreign_key_table},
      _primary_key_columns{primary_key_columns},
      _primary_key_table{primary_key_table} {
  Assert(_foreign_key_columns.size() == _primary_key_columns.size(),
         "Invalid number of columns for ForeignKeyConstraint.");
  // In general, ForeignKeyConstraints should reference the foreign key table and the primary key table. They are
  // attached to the foreign key table and also hold a pointer to the primary key table.
  // ForeignKeyConstraints are translated to inclusion dependencies (INDs) in the LQP, which are tracked by the
  // StoredTableNode of the primary key table and its subsequent LQP nodes. For test cases that test IND handling, we
  // enforce specific INDs by adding a ForeignKeyConstraint to a MockNode. Since this MockNode does not relate to an
  // actual table object, we do not enforce a TableKeyConstraint to reference a primary key table. In contrast, this
  // check is subject to the (stored) foreign key table when we add a constraint in `Table::add_soft_constraint(...)`
  // and only assert that the foreign key table is set, which we need to verify if an IND exists in the LQP.
  Assert(foreign_key_table, "ForeignKeyConstraint must reference a table.");

  // For foreign key constraints with the same columns, the order of the columns is relevant since it determines which
  // foreign key column references which primary key columns. For instance, [A.a, A.b] in [B.x, B.y] is equal to
  // [A.b, A.a] in [B.y, B.x], but not equal to [A.a, A.b] in [B.y, B.x]. See the following example:
  //
  //      A           B
  //    a | b       x | y
  //   -------     -------
  //    1 | u       1 | u
  //    2 | u       2 | u
  //
  // In this case, [A.a, A.b] in [B.x, B.y]/[A.b, A.a] in [B.y, B.x] holds: We find the tuples (1,u) and (2,u) in A and
  // in B. If we project both tables s.t. the columns are flipped (so A has columns a,b and B has columns x,y), we
  // preserve this property: (u,1) and (u,2) are part of both (projected) relations. However, it is a difference when we
  // flip table A but not table B: Now, the tuples from a look like (u,1) and (u,2) and the tuples from B are (1,u)
  // and (2,u). Obviously, they are not the same tuples anymore.
  //
  // To guarantee unambiguous dependencies, we order the columns and apply the same permutation to the included columns.
  // Doing so, we obtain the same costraint for [A.a, A.b] in [B.x, B.y] and [A.b, A.a] in [B.y, B.x].
  if (_foreign_key_columns.size() > 1) {
    const auto& permutation = sort_indexes(_foreign_key_columns);
    _foreign_key_columns = apply_permutation(_foreign_key_columns, permutation);
    _primary_key_columns = apply_permutation(_primary_key_columns, permutation);
  }
}

const std::vector<ColumnID>& ForeignKeyConstraint::foreign_key_columns() const {
  return _foreign_key_columns;
}

std::shared_ptr<Table> ForeignKeyConstraint::foreign_key_table() const {
  return _foreign_key_table.lock();
}

const std::vector<ColumnID>& ForeignKeyConstraint::primary_key_columns() const {
  return _primary_key_columns;
}

std::shared_ptr<Table> ForeignKeyConstraint::primary_key_table() const {
  return _primary_key_table.lock();
}

size_t ForeignKeyConstraint::hash() const {
  auto hash = boost::hash_value(foreign_key_table());
  boost::hash_combine(hash, primary_key_table());
  boost::hash_combine(hash, _foreign_key_columns.size());
  boost::hash_combine(hash, _foreign_key_columns);
  boost::hash_combine(hash, _primary_key_columns);

  return hash;
}

bool ForeignKeyConstraint::_on_equals(const AbstractTableConstraint& table_constraint) const {
  DebugAssert(dynamic_cast<const ForeignKeyConstraint*>(&table_constraint),
              "Different table_constraint type should have been caught by AbstractTableConstraint::operator==");
  const auto& rhs = static_cast<const ForeignKeyConstraint&>(table_constraint);
  return foreign_key_table() == rhs.foreign_key_table() && primary_key_table() == rhs.primary_key_table() &&
         _foreign_key_columns == rhs._foreign_key_columns && _primary_key_columns == rhs._primary_key_columns;
}

}  // namespace hyrise

namespace std {

size_t hash<hyrise::ForeignKeyConstraint>::operator()(
    const hyrise::ForeignKeyConstraint& foreign_key_constraint) const {
  return foreign_key_constraint.hash();
}

}  // namespace std
