#include <string>
#include <optional>

#include "concurrency/transaction_manager.hpp"
#include "storage/constraints/unique_checker.hpp"
#include "storage/segment_accessor.hpp"
#include "storage/storage_manager.hpp"
#include "storage/constraints/base_constraint_checker.hpp"
#include "storage/constraints/single_constraint_checker.hpp"
#include "storage/constraints/concatenated_constraint_checker.hpp"

namespace opossum {
  
std::shared_ptr<BaseConstraintChecker> create_constraint_checker(const Table& table, const TableConstraintDefinition& constraint) {
  if (constraint.columns.size() == 1) {
    ColumnID column_id = constraint.columns[0];
    DataType data_type = table.column_data_type(column_id);
    return make_shared_by_data_type<BaseConstraintChecker, SingleConstraintChecker>(data_type, table, constraint);
  } else {
    return std::make_shared<ConcatenatedConstraintChecker>(table, constraint);
  }
}

bool constraint_valid_for(const Table& table, const TableConstraintDefinition& constraint,
                          const CommitID& snapshot_commit_id, const TransactionID& our_tid) {
  const auto checker = create_constraint_checker(table, constraint);
  return checker->isValid(snapshot_commit_id, our_tid);
}

bool all_constraints_valid_for(std::shared_ptr<const Table> table, std::shared_ptr<const Table> table_to_insert, const CommitID& snapshot_commit_id, const TransactionID& our_tid) {
  for (const auto& constraint : table->get_unique_constraints()) {
    const auto checker = create_constraint_checker(*table, constraint);
    if (!checker->isValidForInsertedValues(table_to_insert, snapshot_commit_id, our_tid)) {
      return false;
    }
  }
  return true;
}

bool all_constraints_valid_for(const std::string& table_name, std::shared_ptr<const Table> table_to_insert,
                               const CommitID& snapshot_commit_id, const TransactionID& our_tid) {
  auto const table = StorageManager::get().get_table(table_name);
  return all_constraints_valid_for(table, table_to_insert, snapshot_commit_id, our_tid);
}

bool check_constraints_in_commit_range(const std::string& table_name, std::vector<std::shared_ptr<const Table>> tables_to_insert, const CommitID& begin_snapshot_commit_id, const CommitID& end_snapshot_commit_id,
                               const TransactionID& our_tid) {
  auto const table = StorageManager::get().get_table(table_name);
  return false;
  //return all_constraints_valid_for(table, end_snapshot_commit_id, our_tid);
}

bool check_constraints_for_values(const std::string& table_name, std::shared_ptr<const Table> table_to_insert, const CommitID& snapshot_commit_id, const TransactionID& our_tid) {
  auto const table = StorageManager::get().get_table(table_name);
  return all_constraints_valid_for(table, table_to_insert, snapshot_commit_id, our_tid);
} 

}  // namespace opossum
