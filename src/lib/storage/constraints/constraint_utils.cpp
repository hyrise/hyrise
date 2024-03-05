#include "constraint_utils.hpp"

#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "storage/constraints/foreign_key_constraint.hpp"
#include "storage/constraints/table_key_constraint.hpp"
#include "storage/constraints/table_order_constraint.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace {

using namespace hyrise;  // NOLINT(build/namespaces)

std::vector<ColumnID> column_ids_by_name(const std::shared_ptr<Table>& table, const std::vector<std::string>& columns) {
  Assert(table, "Expected table to resolve ColumnIDs.");
  auto column_ids = std::vector<ColumnID>{};
  column_ids.reserve(columns.size());

  for (const auto& column : columns) {
    column_ids.emplace_back(table->column_id_by_name(column));
  }

  return column_ids;
}

std::set<ColumnID> column_ids_by_name(const std::shared_ptr<Table>& table, const std::set<std::string>& columns) {
  Assert(table, "Expected table to resolve ColumnIDs.");
  auto column_ids = std::set<ColumnID>{};

  for (const auto& column : columns) {
    [[maybe_unused]] const auto success = column_ids.emplace(table->column_id_by_name(column)).second;
    DebugAssert(success, "Column '" + column + "' is already part of the constraint.");
  }

  return column_ids;
}

void key_constraint(const std::shared_ptr<Table>& table, const std::set<std::string>& columns,
                    const KeyConstraintType type) {
  auto column_ids = column_ids_by_name(table, columns);
  table->add_soft_constraint(TableKeyConstraint{std::move(column_ids), type});
}

}  // namespace

namespace hyrise {

void primary_key_constraint(const std::shared_ptr<Table>& table, const std::set<std::string>& columns) {
  key_constraint(table, columns, KeyConstraintType::PRIMARY_KEY);
}

void unique_constraint(const std::shared_ptr<Table>& table, const std::set<std::string>& columns) {
  key_constraint(table, columns, KeyConstraintType::UNIQUE);
}

void foreign_key_constraint(const std::shared_ptr<Table>& foreign_key_table,
                            const std::vector<std::string>& foreign_key_columns,
                            const std::shared_ptr<Table>& primary_key_table,
                            const std::vector<std::string>& primary_key_columns) {
  auto foreign_key_column_ids = column_ids_by_name(foreign_key_table, foreign_key_columns);
  auto primary_key_column_ids = column_ids_by_name(primary_key_table, primary_key_columns);

  foreign_key_table->add_soft_constraint(ForeignKeyConstraint{std::move(foreign_key_column_ids), foreign_key_table,
                                                              std::move(primary_key_column_ids), primary_key_table});
}

void order_constraint(const std::shared_ptr<Table>& table, const std::vector<std::string>& ordering_columns,
                      const std::vector<std::string>& ordered_columns) {
  auto ordering_column_ids = column_ids_by_name(table, ordering_columns);
  auto ordered_column_ids = column_ids_by_name(table, ordered_columns);

  table->add_soft_constraint(TableOrderConstraint{std::move(ordering_column_ids), std::move(ordered_column_ids)});
}

}  // namespace hyrise
