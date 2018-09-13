#include "abstract_recoverer.hpp"

#include "concurrency/transaction_manager.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/load_table.hpp"

namespace opossum {

void AbstractRecoverer::_redo_transaction(std::map<TransactionID, std::vector<LoggedItem>>& transactions,
                                          TransactionID transaction_id) {
  auto transaction = transactions.find(transaction_id);
  if (transaction == transactions.end()) return;

  for (auto& item : transaction->second) {
    auto& table = *StorageManager::get().get_table(item.table_name);
    auto& chunk = *table.get_chunk(item.row_id.chunk_id);

    switch (item.type) {
      case LogType::Value: {
        chunk.append(*item.values);

        DebugAssert(chunk.has_mvcc_columns(), "Recovery: Table should have MVCC columns.");
        auto mvcc_columns = chunk.mvcc_columns();
        DebugAssert(mvcc_columns->begin_cids.size() - 1 == item.row_id.chunk_offset,
                    "recovery rowID " + std::to_string(mvcc_columns->begin_cids.size() - 1) + " != logged rowID " +
                        std::to_string(item.row_id.chunk_offset));
        mvcc_columns->begin_cids[mvcc_columns->begin_cids.size() - 1] = 0;
        break;
      }
      case LogType::Invalidation: {
        auto mvcc_columns = chunk.mvcc_columns();
        mvcc_columns->end_cids[item.row_id.chunk_offset] = 0;
        break;
      }
      default:
        Fail("Recovery: Log item type not implemented.");
    }
  }

  transactions.erase(transaction);
}

void AbstractRecoverer::_recover_table(const std::string& path, const std::string& table_name) {
  auto table = load_table(path, Chunk::MAX_SIZE);
  StorageManager::get().add_table(table_name, table);
  ++_number_of_loaded_tables;
}

}  // namespace opossum
