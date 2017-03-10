#include "delete.hpp"

#include <memory>
#include <string>

#include "concurrency/transaction_context.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

Delete::Delete(const std::string& table_name, const std::shared_ptr<const AbstractOperator>& values_to_delete)
    : AbstractReadWriteOperator{values_to_delete}, _table_name{table_name} {}

std::shared_ptr<const Table> Delete::on_execute(TransactionContext* context) {
#ifdef IS_DEBUG
  if (!_execution_input_valid(context)) {
    throw std::runtime_error("Input to Delete isn't valid");
  }
#endif

  _table = StorageManager::get().get_table(_table_name);
  _transaction_id = context->transaction_id();

  const auto values_to_delete = input_table_left();

  for (auto chunk_id = 0u; chunk_id < values_to_delete->chunk_count(); ++chunk_id) {
    const auto& chunk = values_to_delete->get_chunk(chunk_id);

    // we have already verified that all columns reference the same table
    const auto first_column = std::static_pointer_cast<ReferenceColumn>(chunk.get_column(0));
    const auto pos_list = first_column->pos_list();

    _pos_lists.emplace_back(pos_list);

    for (const auto& row_id : *pos_list) {
      auto& referenced_chunk = _table->get_chunk(row_id.chunk_id);

      auto expected = 0u;
      // Actual row lock for delete happens here
      _execute_failed =
          !referenced_chunk.mvcc_columns().tids[row_id.chunk_offset].compare_exchange_strong(expected, _transaction_id);

      // the row is already locked and the transaction needs to be aborted
      if (_execute_failed) return nullptr;
    }
  }

  return nullptr;
}

void Delete::commit(const CommitID cid) {
  for (const auto& pos_list : _pos_lists) {
    for (const auto& row_id : *pos_list) {
      auto& chunk = _table->get_chunk(row_id.chunk_id);

      chunk.mvcc_columns().end_cids[row_id.chunk_offset] = cid;
      chunk.mvcc_columns().tids[row_id.chunk_offset] = 0u;
    }
  }
}

void Delete::abort() {
  for (const auto& pos_list : _pos_lists) {
    for (const auto& row_id : *pos_list) {
      auto& chunk = _table->get_chunk(row_id.chunk_id);

      auto expected = _transaction_id;

      // unlocks all rows locked in on_execute until the operator encountered
      // a row that had already been locked by another transaction
      const auto result = chunk.mvcc_columns().tids[row_id.chunk_offset].compare_exchange_strong(expected, 0u);

      if (!result) return;
    }
  }
}

const std::string Delete::name() const { return "Delete"; }

uint8_t Delete::num_in_tables() const { return 1u; }

/**
* A valid Input needs to be: An existing table with at least one chunk.
* Those chunks need to contain at least one column and each chunk can only reference one table.
*/
bool Delete::_execution_input_valid(const TransactionContext* context) const {
  if (context == nullptr) return false;

  const auto values_to_delete = input_table_left();

  if (!StorageManager::get().has_table(_table_name)) return false;

  const auto table = StorageManager::get().get_table(_table_name);

  if (values_to_delete->chunk_count() == 0u) return false;

  for (auto chunk_id = 0u; chunk_id < values_to_delete->chunk_count(); ++chunk_id) {
    const auto& chunk = input_table_left()->get_chunk(chunk_id);

    if (chunk.col_count() == 0u) return false;

    if (!chunk.references_only_one_table()) return false;

    const auto first_column = std::static_pointer_cast<ReferenceColumn>(chunk.get_column(0u));

    if (table != first_column->referenced_table()) return false;
  }

  return true;
}

}  // namespace opossum
