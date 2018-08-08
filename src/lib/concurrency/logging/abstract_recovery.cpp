#include "abstract_recovery.hpp"

#include "../../storage/storage_manager.hpp"
#include "../../storage/table.hpp"
#include "../transaction_manager.hpp"
#include "types.hpp"
#include "utils/load_table.hpp"

namespace opossum {

void AbstractRecovery::_redo_transactions(const TransactionID& transaction_id, std::vector<LoggedItem>& transactions) {
  for (auto& transaction : transactions) {
    if (transaction.transaction_id != transaction_id) continue;

    auto table = StorageManager::get().get_table(transaction.table_name);
    auto chunk = table->get_chunk(transaction.row_id.chunk_id);

    if (transaction.type == LogType::Value) {
      chunk->append(*transaction.values);

      auto mvcc_columns = chunk->mvcc_columns();
      DebugAssert(mvcc_columns->begin_cids.size() - 1 == transaction.row_id.chunk_offset,
                  "recovery rowID " + std::to_string(mvcc_columns->begin_cids.size() - 1) + " != logged rowID " +
                      std::to_string(transaction.row_id.chunk_offset));
      mvcc_columns->begin_cids[mvcc_columns->begin_cids.size() - 1] = transaction_id;
    } else {
      DebugAssert(transaction.type == LogType::Invalidation, "recovery: transaction type not implemented yet");
      auto mvcc_columns = chunk->mvcc_columns();
      mvcc_columns->end_cids[transaction.row_id.chunk_offset] = transaction_id;
    }
  }

  transactions.erase(std::remove_if(transactions.begin(), transactions.end(),
                                    [&transaction_id](LoggedItem x) { return x.transaction_id == transaction_id; }),
                     transactions.end());
}

void AbstractRecovery::_update_transaction_id(const TransactionID highest_committed_id) {
  if (highest_committed_id > 0) {
    TransactionManager::_reset_to_id(highest_committed_id + 1);
  }
}

void AbstractRecovery::_recover_table(const std::string& path, const std::string& table_name) {
  auto table = load_table(path, Chunk::MAX_SIZE);
  StorageManager::get().add_table(table_name, table);
}

}  // namespace opossum
