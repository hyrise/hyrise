#include "text_recovery.hpp"

#include <fstream>
#include <sstream>

#include "../../operators/insert.hpp"
#include "../../storage/storage_manager.hpp"
#include "../../storage/table.hpp"
#include "../transaction_manager.hpp"
#include "logger.hpp"
#include "types.hpp"

namespace opossum {

enum class LogType { Value, Invalidation };

class LoggedItem {
 public:
  LoggedItem(LogType type, TransactionID& transaction_id, std::string& table_name, RowID& row_id,
             std::vector<AllTypeVariant>& values)
      : type(type), transaction_id(transaction_id), table_name(table_name), row_id(row_id), values(values) {}

  LoggedItem(LogType type, TransactionID& transaction_id, std::string& table_name, RowID& row_id)
      : type(type), transaction_id(transaction_id), table_name(table_name), row_id(row_id) {}

  LogType type;
  TransactionID transaction_id;
  std::string table_name;
  RowID row_id;
  std::optional<std::vector<AllTypeVariant>> values;
};

TextRecovery& TextRecovery::getInstance() {
  static TextRecovery instance;
  return instance;
}

void TextRecovery::recover() {
  TransactionID last_transaction_id{0};
  
  auto log_number = Logger::_get_latest_log_number();
  // for every logfile: read and redo logged entries
  for (auto i = 1u; i < log_number; ++i) {
    std::ifstream log_file(Logger::directory + Logger::filename + std::to_string(i));
    DebugAssert(log_file.is_open(), "Recovery: could not open logfile " + std::to_string(i));

    std::vector<LoggedItem> transactions;

    std::string line;
    while (std::getline(log_file, line)) {
      char log_type = line[1];
      DebugAssert(log_type == 't' || log_type == 'i' || log_type == 'v', "Recovery: invalid log type token");

      // commit
      if (log_type == 't') {
        // line = "(t,<TransactionID>)"
        TransactionID transaction_id = std::stoul(line.substr(3, line.length() - 4));

        // perform transactions
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
          } else if (transaction.type == LogType::Invalidation) {
            auto mvcc_columns = chunk->mvcc_columns();
            mvcc_columns->end_cids[transaction.row_id.chunk_offset] = transaction_id;
          }
        }

        last_transaction_id = std::max(transaction_id, last_transaction_id);

        // TODO: delete elements in transactions vector

      } else {  // 'v' or 'i'
        // There might be a \n in every string, therefore the line could end in every string value
        //                                    
        //        transaction_id_end    table_name_size_end      rowID_end
        //                  v                   v                    v
        // (i,<TransactionID>,<table_name.size()>,<table_name>,<RowID>)
        // (v,<TransactionID>,<table_name.size()>,<table_name>,<RowID>,(<value1.size()>,<value1>,<value2.size()>,...))

        size_t transaction_begin = 3;
        auto transaction_id_end = line.find(',', transaction_begin) - 1;
        DebugAssert(transaction_id_end >= transaction_begin, "Recovery: There is no TransactionID in logfile");
        TransactionID transaction_id = std::stoul(
          line.substr(transaction_begin, transaction_id_end - transaction_begin + 1));

        size_t table_name_size_begin = transaction_id_end + 2;
        auto table_name_size_end = line.find(',', table_name_size_begin) - 1;
        DebugAssert(table_name_size_end >= table_name_size_begin, "Recovery: There is no table_name:size() in logfile");
        size_t table_name_size = std::stoul(
          line.substr(table_name_size_begin, table_name_size_end - table_name_size_begin + 1));

        size_t table_name_begin = table_name_size_end + 2;
        size_t table_name_end = table_name_begin + table_name_size - 1;
        // while table_name contains \n
        while (line.length() <= table_name_end) {
          std::string temp_line;
          std::getline(log_file, temp_line);
          DebugAssert(!log_file.eof(), "Recovery: End of file reached unexpectedly");
          line += "\n" + temp_line;
        }
        std::string table_name = line.substr(table_name_begin, table_name_end - table_name_begin + 1);

        // <RowID> = RowID(<chunk_id>,<chunk_offset>)
        // "RowID(".length() = 6
        size_t chunk_id_begin = table_name_end + 2 + 6;
        size_t chunk_id_end = line.find(",", chunk_id_begin) - 1;
        DebugAssert(chunk_id_end >= chunk_id_begin, "Recovery: There is no chunk_id in logfile");
        ChunkID chunk_id(std::stoul(line.substr(chunk_id_begin, chunk_id_end - chunk_id_begin + 1)));
        size_t chunk_offset_begin = chunk_id_end + 2;
        size_t chunk_offset_end = line.find(")", chunk_offset_begin) - 1;
        DebugAssert(chunk_offset_end >= chunk_offset_begin, "Recovery: There is no chunk_offset in logfile");
        ChunkOffset chunk_offset(std::stoul(
          line.substr(chunk_offset_begin, chunk_offset_end - chunk_offset_begin + 1)));
        RowID row_id{chunk_id, chunk_offset};

        if (log_type == 'i') {
          transactions.push_back(LoggedItem(LogType::Invalidation, transaction_id, table_name, row_id));
          continue;
        }

        break;  // TODO: REMOVE

        // // for value inserts 'v' only
        // auto table = StorageManager::get().get_table(table_name);
        // auto data_types = table->column_data_types();

        // std::vector<AllTypeVariant> values;

        // // (v,12,table1,RowID(0,25),(25,LAND_O,9001,asdf)) -> 25,LAND_O,9001,asdf
        // auto value_string = line.substr(rowID_end + 3, line.length() - rowID_end - 5);
        // size_t position;
        // for (auto& data_type : data_types) {
        //   position = value_string.find(',');
        //   if (position != std::string::npos)
        //     values.push_back(value_string.substr(0, position));
        //   else
        //     values.push_back(value_string);
        //   value_string.erase(0, position + 1);
        //   (void)data_type;  // TODO REMOVE THIS SHIT
        // }

        // transactions.push_back(LoggedItem(LogType::Value, transaction_id, table_name, row_id, values));
      }
    }  // while there is a line end
  }  // for every logfile end

  if (last_transaction_id > 0) {
    ++last_transaction_id;
    TransactionManager::_reset_to_id(last_transaction_id);
  }
}

}  // namespace opossum
