#include "foreign_key_constraint.hpp"

#include <numeric>

#include <boost/container_hash/hash.hpp>

namespace {

using namespace hyrise;  // NOLINT(build/namespaces)

// ForeignKeyConstraints with swapped columns are equivalent. To ensure they are represented the same way, we determine
// the permutation that sorts `columns` and apply it to `columns` and `foreign_key_columns`. Idea taken from
// https://stackoverflow.com/a/17074810
std::vector<size_t> sort_permutation(const std::vector<ColumnID>& column_ids) {
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

ForeignKeyConstraint::ForeignKeyConstraint(const std::vector<ColumnID>& columns,
                                           const std::vector<ColumnID>& foreign_key_columns,
                                           const std::shared_ptr<Table>& table,
                                           const std::shared_ptr<Table>& foreign_key_table)
    : AbstractTableConstraint{columns},
      _foreign_key_columns{foreign_key_columns},
      _table{table},
      _foreign_key_table{foreign_key_table} {
  Assert(_columns.size() == _foreign_key_columns.size(), "Invalid number of columns for ForeignKeyConstraint.");
  Assert(table && foreign_key_table, "ForeignKeyConstraint must reference two tables.");

  // For INDs with the same columns, the order of the expressions is relevant. For instance, [A.a, A.b] in [B.x, B.y] is
  // equals to [A.b, A.a] in [B.y, B.x], but not equals to [A.a, A.b] in [B.y, B.x]. To guarantee unambiguous
  // dependencies, we order the columns and apply the same permutation to the included columns. Doing so, we obtain the
  // same costraint for [A.a, A.b] in [B.x, B.y] and [A.b, A.a] in [B.y, B.x].
  if (columns.size() > 1) {
    const auto& permutation = sort_permutation(_columns);
    _columns = apply_permutation(_columns, permutation);
    _foreign_key_columns = apply_permutation(_foreign_key_columns, permutation);
  }
}

const std::vector<ColumnID>& ForeignKeyConstraint::foreign_key_columns() const {
  return _foreign_key_columns;
}

const std::shared_ptr<Table> ForeignKeyConstraint::table() const {
  return _table.lock();
}

const std::shared_ptr<Table> ForeignKeyConstraint::foreign_key_table() const {
  return _foreign_key_table.lock();
}

size_t ForeignKeyConstraint::hash() const {
  auto hash = boost::hash_value(table());
  boost::hash_combine(hash, foreign_key_table());
  boost::hash_combine(hash, _columns.size());
  boost::hash_combine(hash, _columns);
  boost::hash_combine(hash, _foreign_key_columns);

  return hash;
}

bool ForeignKeyConstraint::_on_equals(const AbstractTableConstraint& table_constraint) const {
  DebugAssert(dynamic_cast<const ForeignKeyConstraint*>(&table_constraint),
              "Different table_constraint type should have been caught by AbstractTableConstraint::operator==");
  const auto& foreign_key_constraint = static_cast<const ForeignKeyConstraint&>(table_constraint);
  return table() == foreign_key_constraint.table() &&
         foreign_key_table() == foreign_key_constraint.foreign_key_table() &&
         _foreign_key_columns == foreign_key_constraint._foreign_key_columns;
}

}  // namespace hyrise

namespace std {

size_t hash<hyrise::ForeignKeyConstraint>::operator()(
    const hyrise::ForeignKeyConstraint& foreign_key_constraint) const {
  return foreign_key_constraint.hash();
}

}  // namespace std
