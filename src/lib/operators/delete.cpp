#include "delete.hpp"

#include <memory>
#include <string>

#include "concurrency/transaction_context.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/reference_column.hpp"
#include "storage/storage_manager.hpp"
#include "utils/assert.hpp"

namespace opossum {

Delete::Delete(const std::string& table_name, const std::shared_ptr<const AbstractOperator>& values_to_delete)
    : AbstractReadWriteOperator{OperatorType::Delete, values_to_delete},
      _table_name{table_name},
      _transaction_id{0},
      _num_rows_deleted{0} {}

const std::string Delete::name() const { return "Delete"; }

std::shared_ptr<const Table> Delete::_on_execute(std::shared_ptr<TransactionContext> context) {
  DebugAssert(_execution_input_valid(context), "Input to Delete isn't valid");

  context->register_read_write_operator(std::static_pointer_cast<AbstractReadWriteOperator>(shared_from_this()));

  _table = StorageManager::get().get_table(_table_name);
  _transaction_id = context->transaction_id();

  const auto values_to_delete = input_table_left();

  for (ChunkID chunk_id{0}; chunk_id < values_to_delete->chunk_count(); ++chunk_id) {
    const auto chunk = values_to_delete->get_chunk(chunk_id);

    // we have already verified that all columns reference the same table
    const auto first_column = std::static_pointer_cast<const ReferenceColumn>(chunk->get_column(ColumnID{0}));
    const auto pos_list = first_column->pos_list();

    _pos_lists.emplace_back(pos_list);

    for (const auto& row_id : *pos_list) {
      auto referenced_chunk = _table->get_chunk(row_id.chunk_id);

      auto expected = 0u;
      // Actual row lock for delete happens here
      const auto success =
          referenced_chunk->get_scoped_mvcc_columns_lock()->tids[row_id.chunk_offset].compare_exchange_strong(
              expected, _transaction_id);

      // the row is already locked and the transaction needs to be rolled back
      if (!success) {
        _mark_as_failed();
        return nullptr;
      }
    }
  }

  _num_rows_deleted = input_table_left()->row_count();

  return nullptr;
}

void Delete::_on_commit_records(const CommitID cid) {
  for (const auto& pos_list : _pos_lists) {
    for (const auto& row_id : *pos_list) {
      auto chunk = _table->get_chunk(row_id.chunk_id);

      chunk->get_scoped_mvcc_columns_lock()->end_cids[row_id.chunk_offset] = cid;
      // We do not unlock the rows so subsequent transactions properly fail when attempting to update these rows.
    }
  }
}

void Delete::_finish_commit() {
  const auto table_statistics = _table->table_statistics();
  if (table_statistics) {
    _table->set_table_statistics(std::make_shared<TableStatistics>(table_statistics->table_type(),
                                                                   table_statistics->row_count() - _num_rows_deleted,
                                                                   table_statistics->column_statistics()));
  }
}

void Delete::_on_rollback_records() {
  for (const auto& pos_list : _pos_lists) {
    for (const auto& row_id : *pos_list) {
      auto chunk = _table->get_chunk(row_id.chunk_id);

      auto expected = _transaction_id;

      // unlock all rows locked in _on_execute
      const auto result =
          chunk->get_scoped_mvcc_columns_lock()->tids[row_id.chunk_offset].compare_exchange_strong(expected, 0u);

      // If the above operation fails, it means the row is locked by another transaction. This must have been
      // the reason why the rollback was initiated. Since _on_execute stopped at this row, we can stop
      // unlocking rows here as well.
      if (!result) return;
    }
  }
}

/**
 * values_to_delete must be a table with at least one chunk, containing at least one ReferenceColumn
 * that all reference the table specified by table_name.
 */
bool Delete::_execution_input_valid(const std::shared_ptr<TransactionContext>& context) const {
  if (context == nullptr) return false;

  const auto values_to_delete = input_table_left();

  if (!StorageManager::get().has_table(_table_name)) return false;

  const auto table = StorageManager::get().get_table(_table_name);

  if (values_to_delete->chunk_count() == 0u) return false;

  for (ChunkID chunk_id{0}; chunk_id < values_to_delete->chunk_count(); ++chunk_id) {
    const auto chunk = values_to_delete->get_chunk(chunk_id);

    if (chunk->column_count() == 0u) return false;

    if (!chunk->references_exactly_one_table()) return false;

    const auto first_column = std::static_pointer_cast<const ReferenceColumn>(chunk->get_column(ColumnID{0}));

    if (table != first_column->referenced_table()) return false;
  }

  return true;
}

std::shared_ptr<AbstractOperator> Delete::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<Delete>(_table_name, copied_input_left);
}

void Delete::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

}  // namespace opossum
