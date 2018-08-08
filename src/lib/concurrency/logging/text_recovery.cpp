#include "text_recovery.hpp"

#include <fstream>
#include <sstream>

#include "../../operators/insert.hpp"
#include "../../storage/storage_manager.hpp"
#include "../../storage/table.hpp"
#include "../../storage/chunk.hpp"
#include "../../storage/storage_manager.hpp"
#include "../../utils/load_table.hpp"
#include "../transaction_manager.hpp"
#include "logger.hpp"
#include "types.hpp"

namespace opossum {

TextRecovery& TextRecovery::getInstance() {
  static TextRecovery instance;
  return instance;
}

std::string TextRecovery::_get_substr_and_incr_begin(const std::string line, size_t& begin, const size_t end) {
  auto token = line.substr(begin, end - begin + 1);
  begin = end + 2;
  return token;
}

// returns substring until delimiter is found and sets begin to begin of next token: the position after delimiter
std::string TextRecovery::_get_substr_and_incr_begin(const std::string line, size_t& begin, const char delimiter) {
  auto end = line.find(delimiter, begin) - 1;
  DebugAssert(end >= begin, "Recovery: Missing token in logfile");
  return _get_substr_and_incr_begin(line, begin, end);
}

// returns string value between begin and end, 
// and sets begin to begin of the next token, assuming there is a delimiter inbetween
std::string TextRecovery::_get_string_value_and_incr_begin(std::string& line, size_t& begin, const size_t end,
  std::ifstream& log_file) {
  // There might be a \n in every string, therefore the line could end in every string value
  // while string contains \n
  while (line.length() <= end) {
    std::string temp_line;
    std::getline(log_file, temp_line);
    DebugAssert(!log_file.eof(), "Recovery: End of file reached unexpectedly");
    line += "\n" + temp_line;
  }
  return _get_substr_and_incr_begin(line, begin, end);
}

std::string TextRecovery::_get_next_value_with_preceding_size_and_incr_begin(std::string& line, size_t& begin, 
  const char delimiter, std::ifstream& log_file) {
  size_t size = std::stoul(_get_substr_and_incr_begin(line, begin, delimiter));
  size_t end = begin + size - 1;
  return _get_string_value_and_incr_begin(line, begin, end, log_file);
}

void TextRecovery::recover() {
  TransactionID last_transaction_id{0};
  for (auto& path: Logger::get_all_log_file_paths()) {
    std::ifstream log_file(path);
    DebugAssert(log_file.is_open(), "Recovery: could not open logfile " + path);

    std::vector<LoggedItem> transactions;

    std::string line;
    while (std::getline(log_file, line)) {
      // next_token_begin
      //    v
      // (l,<path.size()>,<path>,<table_name.size(),<table_name>)
      // (t,<TransactionID>)
      // (i,<TransactionID>,<table_name.size()>,<table_name>,<RowID>)
      // (v,<TransactionID>,<table_name.size()>,<table_name>,<RowID>,(<value1.size()>,<value1>,<value2.size()>,...))

      // next_token_begin is set to the pos of the next token after each read
      size_t next_token_begin = 3;

      char log_type = line[1];
      DebugAssert(log_type == 't' || log_type == 'i' || log_type == 'v' || log_type == 'l',
        "Recovery: invalid log type token");

      // if commit entry
      if (log_type == 't') {
        TransactionID transaction_id = std::stoul(_get_substr_and_incr_begin(line, next_token_begin, ')'));
        _redo_transactions(transaction_id, transactions);
        last_transaction_id = std::max(transaction_id, last_transaction_id);
        continue;
      } 

      // if load table entry
      if (log_type == 'l') {
        std::string path = _get_next_value_with_preceding_size_and_incr_begin(line, next_token_begin, ',', log_file);
        std::string table_name = _get_next_value_with_preceding_size_and_incr_begin(line, next_token_begin, ',', log_file);
        DebugAssert(line[next_token_begin - 1] == ')', "Recovery: load table entry expected ')', but got " + 
          line[next_token_begin - 1] + " instead.");

        _recover_table(path, table_name);
        continue;
      }
      
      // else: value or invalidation entry
                                  
      TransactionID transaction_id = std::stoul(_get_substr_and_incr_begin(line, next_token_begin, ','));

      std::string table_name = _get_next_value_with_preceding_size_and_incr_begin(
        line, next_token_begin, ',', log_file);

      // <RowID> = RowID(<chunk_id>,<chunk_offset>)
      // "RowID(".length() = 6
      next_token_begin += 6;
      ChunkID chunk_id(std::stoul(_get_substr_and_incr_begin(line, next_token_begin, ',')));
      ChunkOffset chunk_offset(std::stoul(_get_substr_and_incr_begin(line, next_token_begin, ')')));
      RowID row_id{chunk_id, chunk_offset};

      // if invalidation
      if (log_type == 'i') {
        transactions.emplace_back(LoggedItem(LogType::Invalidation, transaction_id, table_name, row_id));
        continue;
      }

      // else: value entry
      // (<value1.size()>,<value1>,<value2.size()>,...)

      // adding two braces of "chunk_offset_end),(value_size_begin"
      next_token_begin += 2;

      DebugAssert(line[next_token_begin - 2] == ',', 
        "Recovery: Expected ',' but got '" + line[next_token_begin - 2] + "' instead")
      DebugAssert(line[next_token_begin - 1] == '(', 
        "Recovery: Expected '(' but got '" + line[next_token_begin - 1] + "' instead")
      
      std::vector<AllTypeVariant> values;
      // while still values in line
      while (line[next_token_begin - 1] != ')') {
        values.emplace_back(_get_next_value_with_preceding_size_and_incr_begin(line, next_token_begin, ',', log_file));
        DebugAssert(line.length() >= next_token_begin, "Recovery: line ended before ')'");
      }

      transactions.emplace_back(LoggedItem(LogType::Value, transaction_id, table_name, row_id, values));

    }  // while there is a line
  }  // for every logfile

  _update_transaction_id(last_transaction_id);
}

}  // namespace opossum
