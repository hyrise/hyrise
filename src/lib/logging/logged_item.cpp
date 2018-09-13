#include "logged_item.hpp"

#include "concurrency/transaction_manager.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

LoggedItem::LoggedItem(TransactionID& transaction_id, std::string& table_name, RowID& row_id)
  : _transaction_id(transaction_id), _table_name(table_name), _row_id(row_id) {}

LoggedInvalidation::LoggedInvalidation(TransactionID& transaction_id, std::string& table_name, RowID& row_id)
  : LoggedItem(transaction_id, table_name, row_id) {}

LoggedValue::LoggedValue(TransactionID& transaction_id, std::string& table_name, RowID& row_id, 
                         std::vector<AllTypeVariant>& values)
  : LoggedItem(transaction_id, table_name, row_id), _values(values) {}

void LoggedInvalidation::redo() {
  auto& chunk = *StorageManager::get().get_table(_table_name)->get_chunk(_row_id.chunk_id);

  DebugAssert(chunk.has_mvcc_columns(), "Recovery: Table should have MVCC columns.");
  auto mvcc_columns = chunk.mvcc_columns();
  mvcc_columns->end_cids[_row_id.chunk_offset] = 0;
}

void LoggedValue::redo() {
  auto& chunk = *StorageManager::get().get_table(_table_name)->get_chunk(_row_id.chunk_id);

  chunk.append(_values);

  DebugAssert(chunk.has_mvcc_columns(), "Recovery: Table should have MVCC columns.");
  auto mvcc_columns = chunk.mvcc_columns();

  DebugAssert(mvcc_columns->begin_cids.size() - 1 == _row_id.chunk_offset,
              "recovery rowID " + std::to_string(mvcc_columns->begin_cids.size() - 1) + " != logged rowID " +
                  std::to_string(_row_id.chunk_offset));
  mvcc_columns->begin_cids[mvcc_columns->begin_cids.size() - 1] = 0;
}

}  // namespace opossum
