#include "table_inclusion_constraint.hpp"

#include <numeric>

namespace {

using namespace hyrise;  // NOLINT(build/namespaces)

std::vector<size_t> sort_permutation(const std::vector<ColumnID>& column_ids) {
  auto permutation = std::vector<size_t>(column_ids.size());
  // Fill permutation with [0, 1, ..., n - 1] and order the permutation by sorting column_ids.
  std::iota(permutation.begin(), permutation.end(), 0);
  std::sort(permutation.begin(), permutation.end(),
            [&](auto lhs, auto rhs) { return column_ids[lhs] < column_ids[rhs]; });
  return permutation;
}

void apply_permutation_in_place(std::vector<ColumnID>& column_ids, const std::vector<size_t>& permutation) {
  const auto column_count = column_ids.size();
  auto done = std::vector<bool>(column_count);

  for (auto column_idx = size_t{0}; column_idx < column_count; ++column_idx) {
    if (done[column_idx]) {
      continue;
    }

    done[column_idx] = true;
    auto original_position = column_idx;
    auto target_position = permutation[column_idx];
    while (column_idx != target_position) {
      std::swap(column_ids[original_position], column_ids[target_position]);
      done[target_position] = true;
      original_position = target_position;
      target_position = permutation[target_position];
    }
  }
}

}  // namespace

namespace hyrise {

TableInclusionConstraint::TableInclusionConstraint(const std::vector<ColumnID>& columns,
                                                   const std::vector<ColumnID>& included_columns,
                                                   const std::string& included_table_name)
    : AbstractTableConstraint{columns}, _included_columns{included_columns}, _included_table_name{included_table_name} {
  Assert(_columns.size() == _included_columns.size(), "Invalid number of columns for TableInclusionConstraint");

  // For INDs with the same columns, the order of the expressions is relevant. For instance, [A.a, A.b] in [B.x, B.y] is
  // equals to [A.b, A.a] in [B.y, B.x], but not equals to [A.a, A.b] in [B.y, B.x]. To guarantee unambiguous
  // dependencies, we order the columns and apply the same permutation to the included columns. Doing so, we obtain the
  // same costraint for [A.a, A.b] in [B.x, B.y] and [A.b, A.a] in [B.y, B.x].
  const auto& permutation = sort_permutation(_columns);
  apply_permutation_in_place(_columns, permutation);
  apply_permutation_in_place(_included_columns, permutation);
}

const std::vector<ColumnID>& TableInclusionConstraint::included_columns() const {
  return _included_columns;
}

size_t TableInclusionConstraint::hash() const {
  auto hash = boost::hash_value(_columns.size());
  boost::hash_combine(hash, _columns);
  boost::hash_combine(hash, _included_table_name);
  boost::hash_combine(hash, _included_columns);

  return hash;
}

const std::string& TableInclusionConstraint::included_table_name() const {
  return _included_table_name;
}

bool TableInclusionConstraint::_on_equals(const AbstractTableConstraint& table_constraint) const {
  DebugAssert(dynamic_cast<const TableInclusionConstraint*>(&table_constraint),
              "Different table_constraint type should have been caught by AbstractTableConstraint::operator==");
  const auto& inclusion_constraint = static_cast<const TableInclusionConstraint&>(table_constraint);
  return _included_table_name == inclusion_constraint._included_table_name &&
         _included_columns == inclusion_constraint._included_columns;
}

}  // namespace hyrise

namespace std {

size_t hash<hyrise::TableInclusionConstraint>::operator()(
    const hyrise::TableInclusionConstraint& inclusion_constraint) const {
  return inclusion_constraint.hash();
}

}  // namespace std
