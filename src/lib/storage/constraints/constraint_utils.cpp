#include "constraint_utils.hpp"

#include <cstdint>
#include <iterator>
#include <memory>
#include <ranges>
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

bool key_constraint_is_confidently_valid(const std::shared_ptr<Table>& table,
                                         const TableKeyConstraint& table_key_constraint) {
  if (!table_key_constraint.can_become_invalid()) {
    return true;
  }

  if (table_key_constraint.last_validation_result() == ValidationResultType::INVALID) {
    return false;
  }

  const auto last_validated_on = table_key_constraint.last_validated_on();
  const auto chunk_count = table->chunk_count();
  // Due to Hyrise being a append-only database the most recent chunks are the last ones added to the table. Therefore
  // we iterate backwards through all chunks of the table to potentially return faster.
  for (auto prev_chunk_id = static_cast<int32_t>(chunk_count - 1); prev_chunk_id >= 0; --prev_chunk_id) {
    const auto source_chunk = table->get_chunk(static_cast<ChunkID>(prev_chunk_id));

    // We use `max_begin_cid` here. This can lead to overly pessimistic results, but as of right now we don't have a
    // better way to determine the last valid commit id here.
    const auto max_begin_cid = source_chunk->mvcc_data()->max_begin_cid.load();
    if (max_begin_cid != MAX_COMMIT_ID && max_begin_cid > last_validated_on) {
      return false;
    }
  }

  return true;
}

bool key_constraint_is_confidently_invalid(const std::shared_ptr<Table>& table,
                                           const TableKeyConstraint& table_key_constraint) {
  if (table_key_constraint.last_validation_result() == ValidationResultType::VALID) {
    return false;
  }

  const auto last_invalidated_on = table_key_constraint.last_invalidated_on();
  const auto chunk_count = table->chunk_count();
  // Due to Hyrise being a append-only database the most recent chunks are the last ones added to the table. Therefore
  // we iterate backwards through all chunks of the table to potentially return faster.
  for (auto prev_chunk_id = static_cast<int32_t>(chunk_count - 1); prev_chunk_id >= 0; --prev_chunk_id) {
    const auto source_chunk = table->get_chunk(static_cast<ChunkID>(prev_chunk_id));

    const auto max_end_cid = source_chunk->mvcc_data()->max_end_cid.load();
    if (max_end_cid != MAX_COMMIT_ID && max_end_cid > last_invalidated_on) {
      return false;
    }
  }

  return true;
}

bool column_is_unique(const std::shared_ptr<Table>& table, const ColumnID column_id) {
  DebugAssert(column_id < table->column_count(), "ColumnID out of range.");
  for (const auto& key_constraint : table->soft_key_constraints()) {
    if (key_constraint.can_become_invalid()) {
      continue;
    }

    const auto& key_type = key_constraint.key_type();
    if (key_type != KeyConstraintType::PRIMARY_KEY && key_type != KeyConstraintType::UNIQUE) {
      continue;
    }

    const auto& columns = key_constraint.columns();
    if (columns.size() == 1 && columns.contains(column_id)) {
      return true;
    }
  }

  return false;
}

std::vector<bool> columns_are_unique(const std::shared_ptr<Table>& table) {
  const auto column_count = table->column_count();
  std::vector<bool> are_unique(column_count);
  for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
    are_unique[column_id] = column_is_unique(table, column_id);
  }
  return are_unique;
}

bool column_is_key(const std::shared_ptr<Table>& table, const ColumnID column_id) {
  DebugAssert(column_id < table->column_count(), "ColumnID out of range.");
  for (const auto& key_constraint : table->soft_key_constraints()) {
    if (key_constraint.can_become_invalid()) {
      continue;
    }

    const auto& key_type = key_constraint.key_type();
    if (key_type != KeyConstraintType::PRIMARY_KEY) {
      continue;
    }

    const auto& columns = key_constraint.columns();
    if (columns.contains(column_id)) {
      return true;
    }
  }

  for (const auto& fk_constraint : table->soft_foreign_key_constraints()) {
    if (fk_constraint.foreign_key_table() != table) {
      continue;
    }

    const auto& columns = fk_constraint.foreign_key_columns();
    if (std::ranges::find(columns, column_id) != columns.end()) {
      return true;
    }
  }

  return false;
}

std::vector<bool> columns_are_key(const std::shared_ptr<Table>& table) {
  const auto column_count = table->column_count();
  std::vector<bool> are_key(column_count);
  for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
    are_key[column_id] = column_is_key(table, column_id);
  }
  return are_key;
}

bool column_might_be_unique(const std::shared_ptr<Table>& table, const ColumnID column_id) {
  DebugAssert(column_id < table->column_count(), "ColumnID out of range.");
  for (const auto& key_constraint : table->soft_key_constraints()) {
    if (key_constraint.can_become_invalid()) {
      continue;
    }

    const auto& key_type = key_constraint.key_type();
    if (key_type != KeyConstraintType::PRIMARY_KEY && key_type != KeyConstraintType::UNIQUE) {
      continue;
    }

    const auto& columns = key_constraint.columns();
    if (columns.contains(column_id)) {
      return true;
    }
  }

  return false;
}

std::vector<bool> columns_might_be_unique(const std::shared_ptr<Table>& table) {
  const auto column_count = table->column_count();
  std::vector<bool> might_be_unique(column_count);
  for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
    might_be_unique[column_id] = column_might_be_unique(table, column_id);
  }
  return might_be_unique;
}

}  // namespace hyrise
