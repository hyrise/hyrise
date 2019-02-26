#include <optional>
#include <string>

#include "concurrency/transaction_manager.hpp"
#include "operators/validate.hpp"
#include "storage/constraints/base_constraint_checker.hpp"
#include "storage/constraints/concatenated_constraint_checker.hpp"
#include "storage/constraints/single_constraint_checker.hpp"
#include "storage/constraints/unique_checker.hpp"
#include "storage/segment_accessor.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

std::shared_ptr<BaseConstraintChecker> create_constraint_checker(const Table& table,
                                                                 const TableConstraintDefinition& constraint) {
  if (constraint.columns.size() == 1) {
    const ColumnID column_id = constraint.columns[0];
    const DataType data_type = table.column_data_type(column_id);
    return make_shared_by_data_type<BaseConstraintChecker, SingleConstraintChecker>(data_type, table, constraint);
  } else {
    return std::make_shared<ConcatenatedConstraintChecker>(table, constraint);
  }
}

bool constraint_satisfied(const Table& table, const TableConstraintDefinition& constraint,
                          const CommitID snapshot_commit_id, const TransactionID our_tid) {
  const auto checker = create_constraint_checker(table, constraint);
  const auto& [satisfied, _] = checker->is_valid(snapshot_commit_id, our_tid);
  return satisfied;
}

std::tuple<bool, ChunkID> constraints_satisfied_for_values(const std::string& table_name,
                                                           std::shared_ptr<const Table> table_to_insert,
                                                           const CommitID snapshot_commit_id,
                                                           const TransactionID our_tid, const ChunkID since) {
  const auto table = StorageManager::get().get_table(table_name);
  ChunkID first_value_segment;
  for (const auto& constraint : table->get_unique_constraints()) {
    const auto checker = create_constraint_checker(*table, constraint);
    const auto& [valid, i] = checker->is_valid_for_inserted_values(table_to_insert, snapshot_commit_id, our_tid, since);
    first_value_segment = i;
    if (!valid) {
      return std::make_tuple<>(false, ChunkID{0});
    }
  }
  return std::make_tuple<>(true, first_value_segment);
}

}  // namespace opossum
