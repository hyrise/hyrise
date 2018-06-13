#include "binary_recovery.hpp"

#include "logger.hpp"
#include "types.hpp"
#include "../../storage/storage_manager.hpp"
#include "../../storage/table.hpp"
#include "../../operators/insert.hpp"
#include "../transaction_manager.hpp"

#include <fstream>
#include <sstream>

namespace opossum {

enum class LogType {Value, Invalidation};

class LoggedItem {
 public:
  LoggedItem(LogType type, TransactionID &transaction_id, std::string &table_name, RowID &row_id, std::vector<AllTypeVariant> &values)
  : type(type)
  , transaction_id(transaction_id)
  , table_name(table_name)
  , row_id(row_id)
  , values(values) {
  };

  LoggedItem(LogType type, TransactionID &transaction_id, std::string &table_name, RowID &row_id)
  : type(type)
  , transaction_id(transaction_id)
  , table_name(table_name)
  , row_id(row_id){
  };

  LogType type;
  TransactionID transaction_id;
  std::string table_name;
  RowID row_id;
  std::optional<std::vector<AllTypeVariant>> values;
};

BinaryRecovery& BinaryRecovery::getInstance() {
  static BinaryRecovery instance;
  return instance;
}

void BinaryRecovery::recover() {
  std::fstream last_log_number_file(Logger::directory + Logger::last_log_filename, std::ios::in);
  uint log_number;
  last_log_number_file >> log_number;
  last_log_number_file.close();

  TransactionID last_transaction_id{0};

  // for every logfile: read and redo logged entries
  for (auto i = 1u; i < log_number; ++i){
    // TODO: check if file exists
    std::ifstream log_file{Logger::directory + Logger::filename + std::to_string(i), std::ios::binary};

    std::vector<LoggedItem> transactions;

    // TransactionID last_transaction_id{0};

    while(true) {
      char log_type;
      log_file.read(&log_type, sizeof(char));

      if (log_file.eof()){ break; }

      TransactionID transaction_id;
      log_file.read(reinterpret_cast<char*>(&transaction_id), sizeof(TransactionID));

      if (log_type == 't'){   // commit 
        /*
        *     Commit Entries:
        *       - log entry type ('t') : sizeof(char)
        *       - transaction_id       : sizeof(TransactionID)
        */

        // TODO refactor: same as text file recovery
        for (auto &transaction : transactions) {
          if (transaction.transaction_id != transaction_id)
            continue;

          auto table = StorageManager::get().get_table(transaction.table_name);
          auto chunk = table->get_chunk(transaction.row_id.chunk_id);

          if (transaction.type == LogType::Value) {
            chunk->append(*transaction.values);

            auto mvcc_columns = chunk->mvcc_columns();
            DebugAssert(mvcc_columns->begin_cids.size() - 1 == transaction.row_id.chunk_offset, "recovery rowID " + std::to_string(mvcc_columns->begin_cids.size() - 1) + " != logged rowID " + std::to_string(transaction.row_id.chunk_offset));
            mvcc_columns->begin_cids[mvcc_columns->begin_cids.size() - 1] = transaction_id;          
          } else if (transaction.type == LogType::Invalidation) {
            auto mvcc_columns = chunk->mvcc_columns();
            mvcc_columns->end_cids[transaction.row_id.chunk_offset] = transaction_id;
          }
        }

        last_transaction_id = std::max(transaction_id, last_transaction_id); 

        // TODO: delete elements in transactions vector

      }
      else { // 'v' or 'i'
        DebugAssert(log_type == 'v' || log_type == 'i', "recovery first token of new entry is neither c, v nor i");
        /*     Invalidation and begin of value entries:
        *       - log entry type       : sizeof(char)
        *       - transaction_id       : sizeof(TransactionID)
        *       - table_name.size()    : sizeof(size_t)             --> what is max table_name size?
        *       - table_name           : table_name.size()
        *       - row_id               : sizeof(ChunkID) + sizeof(ChunkOffset) 
        *  1 + 4 + 8 + 6 + 4 +4 = 27
        */

        size_t table_name_size;
        log_file.read(reinterpret_cast<char*>(&table_name_size), sizeof(size_t));

        std::string table_name(table_name_size, '\0');
        log_file.read(table_name.data(), table_name_size);

        ChunkID chunk_id;
        log_file.read(reinterpret_cast<char*>(&chunk_id), sizeof(ChunkID));

        ChunkOffset chunk_offset;
        log_file.read(reinterpret_cast<char*>(&chunk_offset), sizeof(ChunkOffset));

        RowID row_id(chunk_id, chunk_offset);

        if (log_type == 'i'){
          transactions.push_back(LoggedItem(LogType::Invalidation, transaction_id, table_name, row_id));
          continue;
        }
        else {
          // TODO: insert values
          break;
        }
      }
    }
  }

  if (last_transaction_id > 0) {
    ++last_transaction_id;
    TransactionManager::_reset_to_id(last_transaction_id);
  }

}

}  // namespace opossum
