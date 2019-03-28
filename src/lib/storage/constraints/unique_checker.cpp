#include <optional>
#include <string>

#include "concurrency/transaction_manager.hpp"
#include "operators/validate.hpp"
#include "storage/constraints/base_constraint_checker.hpp"
#include "storage/constraints/multi_column_constraint_checker.hpp"
#include "storage/constraints/single_column_constraint_checker.hpp"
#include "storage/constraints/unique_checker.hpp"
#include "storage/segment_accessor.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

std::shared_ptr<BaseConstraintChecker> create_constraint_checker(const Table& table,
                                                                 const TableConstraintDefinition& constraint) {
  if (constraint.columns.size() == 1) {
    const ColumnID column_id = constraint.columns[0];
    const DataType data_type = table.column_data_type(column_id);
    return make_shared_by_data_type<BaseConstraintChecker, SingleColumnConstraintChecker>(data_type, table, constraint);
  } else {
    return std::make_shared<MultiColumnConstraintChecker>(table, constraint);
  }
}

bool constraint_satisfied(const Table& table, const TableConstraintDefinition& constraint,
                          const CommitID snapshot_commit_id, const TransactionID our_tid) {
  const auto checker = create_constraint_checker(table, constraint);
  const auto satisfied = checker->is_valid(snapshot_commit_id, our_tid);
  return satisfied;
}

std::tuple<bool, ChunkID> check_constraints_for_values(const std::string& table_name,
                                                           const std::shared_ptr<const Table>& table_to_insert,
                                                           const CommitID snapshot_commit_id,
                                                           const TransactionID our_tid, const ChunkID start_chunk_id) {
  const auto table = StorageManager::get().get_table(table_name);
  ChunkID first_chunk_to_check{0};
  for (const auto& constraint : table->get_unique_constraints()) {
    const auto checker = create_constraint_checker(*table, constraint);
    const auto& [valid, chunk_id] = checker->is_valid_for_inserted_values(
      table_to_insert, snapshot_commit_id, our_tid, start_chunk_id);
    first_chunk_to_check = chunk_id;
    if (!valid) {
      return std::make_tuple<>(false, ChunkID{0});
    }
  }
  return std::make_tuple<>(true, first_chunk_to_check);
}

}  // namespace opossum
