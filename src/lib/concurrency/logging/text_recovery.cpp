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

// returns substring until delimiter is found and sets begin to begin of next token: the position after delimiter
std::string TextRecovery::_get_substr_and_incr_begin(const std::string& line, size_t& begin, const char delimiter) {
  auto end = line.find(delimiter, begin) - 1;
  DebugAssert(end >= begin, "Recovery: Missing token in logfile");
  auto token = line.substr(begin, end - begin + 1);
  begin = end + 2;
  return token;
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

      // if commit
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

        continue;
      } 
      
      // else: value or invalidation entry

      // There might be a \n in every string, therefore the line could end in every string value
      //                                    
      //        transaction_id_end    table_name_size_end      rowID_end
      //                  v                   v                    v
      // (i,<TransactionID>,<table_name.size()>,<table_name>,<RowID>)
      // (v,<TransactionID>,<table_name.size()>,<table_name>,<RowID>,(<value1.size()>,<value1>,<value2.size()>,...))


      // size_t transaction_id_begin = 3;
      size_t next_token_begin = 3;
      TransactionID transaction_id = std::stoul(_get_substr_and_incr_begin(line, next_token_begin, ','));

      size_t table_name_size = std::stoul(_get_substr_and_incr_begin(line, next_token_begin, ','));

      size_t table_name_end = next_token_begin + table_name_size - 1;
      // while table_name contains \n
      while (line.length() <= table_name_end) {
        std::string temp_line;
        std::getline(log_file, temp_line);
        DebugAssert(!log_file.eof(), "Recovery: End of file reached unexpectedly");
        line += "\n" + temp_line;
      }
      std::string table_name = line.substr(next_token_begin, table_name_end - next_token_begin + 1);
      
      next_token_begin = table_name_end + 2;

      // <RowID> = RowID(<chunk_id>,<chunk_offset>)
      // "RowID(".length() = 6
      next_token_begin += 6;
      ChunkID chunk_id(std::stoul(_get_substr_and_incr_begin(line, next_token_begin, ',')));
      ChunkOffset chunk_offset(std::stoul(_get_substr_and_incr_begin(line, next_token_begin, ')')));
      RowID row_id{chunk_id, chunk_offset};

      // if invalidation
      if (log_type == 'i') {
        transactions.push_back(LoggedItem(LogType::Invalidation, transaction_id, table_name, row_id));
        continue;
      }
      // else: value entry

      // (<value1.size()>,<value1>,<value2.size()>,...)
      std::vector<AllTypeVariant> values;

      // "chunk_offset_end),(value_size_begin"
      // size_t value_size_begin = chunk_offset_end + 4;
      size_t value_size_begin = next_token_begin + 2;
      DebugAssert(line[value_size_begin - 2] == ',', 
        "Recovery: Expected ',' but got '" + line[value_size_begin - 2] + "' instead")
      DebugAssert(line[value_size_begin - 1] == '(', 
        "Recovery: Expected '(' but got '" + line[value_size_begin - 1] + "' instead")
      
      // while still values in line
      while (line[value_size_begin - 1] != ')') {
        size_t value_size_end = line.find(",", value_size_begin) - 1;
        DebugAssert(value_size_end >= value_size_begin, "Recovery: There is no value_size in logfile");
        size_t value_size = std::stoul(line.substr(value_size_begin, value_size_end - value_size_begin + 1));

        size_t value_begin = value_size_end + 2;
        size_t value_end = value_begin + value_size - 1;
        // while value contains \n
        while (line.length() <= value_end) {
          std::string temp_line;
          std::getline(log_file, temp_line);
          DebugAssert(!log_file.eof(), "Recovery: End of file reached unexpectedly");
          line += "\n" + temp_line;
        }

        values.push_back(line.substr(value_begin, value_size));
        value_size_begin = value_end + 2;
        DebugAssert(line.length() >= value_size_begin, "Recovery: line ended before ')'");
      }

      transactions.push_back(LoggedItem(LogType::Value, transaction_id, table_name, row_id, values));

    }  // while there is a line end
  }  // for every logfile end

  if (last_transaction_id > 0) {
    ++last_transaction_id;
    TransactionManager::_reset_to_id(last_transaction_id);
  }
}

}  // namespace opossum
