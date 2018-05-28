#include "recovery.hpp"

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

Recovery& Recovery::getInstance() {
  static Recovery instance;
  return instance;
}

void Recovery::recover() {
  std::ifstream log_file(Logger::directory + Logger::filename);

  std::vector<LoggedItem> transactions;

  std::string line;
  while (std::getline(log_file, line))
  {
    char log_type = line[1];

    // commit
    if (log_type == 't'){
      TransactionID transaction_id = std::stoull(line.substr(3, line.length() - 4));

      // perform transaction
      for (auto &transaction : transactions) {
        if (transaction.transaction_id != transaction_id)
          continue;

        auto table = StorageManager::get().get_table(transaction.table_name);
        auto chunk = table->get_chunk(transaction.row_id.chunk_id);

        if (transaction.type == LogType::Value) {
          chunk->append(*transaction.values);

          auto mvcc_columns = chunk->mvcc_columns();
          mvcc_columns->grow_by(1, MvccColumns::MAX_COMMIT_ID);
          mvcc_columns->begin_cids[mvcc_columns->begin_cids.size() - 2] = transaction_id;

          mvcc_columns->print();

          DebugAssert(mvcc_columns->begin_cids.size() - 2 == transaction.row_id.chunk_offset, "recovery rowID " + std::to_string(mvcc_columns->begin_cids.size() - 2) + " != logged rowID " + std::to_string(transaction.row_id.chunk_offset));
          continue;
        }

        if (transaction.type == LogType::Invalidation) {
          auto mvcc_columns = chunk->mvcc_columns();
          mvcc_columns->end_cids[transaction.row_id.chunk_offset] = transaction_id;
          mvcc_columns->print();
          continue;
        }

      }

      TransactionManager::get()._last_commit_id = transaction_id;

      // TODO: delete elements in transactions vector

    } else {  // 'v' or 'i'

      // transaction_id_end  rowID_end
      //     |  rowID_position  |
      //     v        v         v
      // (v,12,table1,RowID(0,25),(25,LAND_O,9001,asdf))
      // (i,86,TABLE2,RowID(....))                  

      auto transaction_id_end = line.find(',', 3) - 1;
      TransactionID transaction_id = std::stoull(line.substr(3, transaction_id_end - 2));

      auto rowID_position = line.find(",RowID(") + 1;
      auto rowID_end = line.find(")", rowID_position);
      // ...,RowID(x,y)...  ->   x,y
      auto rowID_substring = line.substr(rowID_position + 6, rowID_end - rowID_position - 6);
      std::istringstream rowID_stream(rowID_substring); 
      ChunkID chunk_id;
      rowID_stream >> chunk_id;
      rowID_stream.ignore();  // ignore ','
      ChunkOffset chunk_offset;
      rowID_stream >> chunk_offset;
      RowID row_id(chunk_id, chunk_offset);

      std::string table_name = line.substr(transaction_id_end + 2, rowID_position - transaction_id_end - 3);

      if (log_type == 'i'){
        transactions.push_back(LoggedItem(LogType::Invalidation, transaction_id, table_name, row_id));
        continue;
      }

      // for value inserts 'v' only
      auto table = StorageManager::get().get_table(table_name);
      auto data_types = table->column_data_types();

      std::vector<AllTypeVariant> values;

      // (v,12,table1,RowID(0,25),(25,LAND_O,9001,asdf)) -> 25,LAND_O,9001,asdf
      auto value_string = line.substr(rowID_end + 3, line.length() - rowID_end - 5);
      size_t position;
      for (auto &data_type : data_types){
        position = value_string.find(',');
        if (position !=  std::string::npos)
          values.push_back(value_string.substr(0, position));
        else
          values.push_back(value_string);
        value_string.erase(0, position + 1);
        (void) data_type; // TODO REMOVE THIS SHIT
      }

      transactions.push_back(LoggedItem(LogType::Value, transaction_id, table_name, row_id, values));
    }
  }
}

}  // namespace opossum
