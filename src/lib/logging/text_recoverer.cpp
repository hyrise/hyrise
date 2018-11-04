#include "text_recoverer.hpp"

#include <fstream>
#include <sstream>

#include "concurrency/transaction_manager.hpp"
#include "logger.hpp"
#include "operators/insert.hpp"
#include "storage/chunk.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/load_table.hpp"

namespace opossum {

std::string TextRecoverer::_extract_token(const std::string& line, size_t& begin, const size_t end) {
  auto token = line.substr(begin, end - begin + 1);
  begin = end + 2;
  return token;
}

std::string TextRecoverer::_extract_token_up_to_delimiter(const std::string& line, size_t& begin,
                                                          const char delimiter) {
  auto end = line.find(delimiter, begin) - 1;
  DebugAssert(end >= begin, "Recoverer: Missing token in logfile");
  return _extract_token(line, begin, end);
}

std::string TextRecoverer::_extract_string_value(std::string& line, size_t& begin, const size_t end,
                                                 std::ifstream& log_file) {
  // There might be a \n in every string, therefore the line could end in every string value
  // while string contains \n
  while (line.length() <= end) {
    std::string temp_line;
    std::getline(log_file, temp_line);
    DebugAssert(!log_file.eof(), "Recoverer: End of file reached unexpectedly");
    line += "\n" + temp_line;
  }
  return _extract_token(line, begin, end);
}

std::string TextRecoverer::_extract_next_value_with_preceding_size(std::string& line, size_t& begin,
                                                                   const char delimiter, std::ifstream& log_file) {
  size_t size = std::stoul(_extract_token_up_to_delimiter(line, begin, delimiter));
  size_t end = begin + size - 1;
  return _extract_string_value(line, begin, end, log_file);
}

uint32_t TextRecoverer::recover() {
  for (auto& path : Logger::get_all_log_file_paths()) {
    std::ifstream log_file(path);
    DebugAssert(log_file.is_open(), "Recoverer: could not open logfile " + path);

    std::map<TransactionID, std::vector<std::unique_ptr<LoggedItem>>> transactions;

    std::string line;
    while (std::getline(log_file, line)) {
      // next_token_begin
      //    v
      // (l,<path.size()>,<path>,<table_name.size(),<table_name>)
      // (t,<TransactionID>)
      // (i,<TransactionID>,<table_name.size()>,<table_name>,<RowID>)
      // (v,<TransactionID>,<table_name.size()>,<table_name>,<RowID>,(<value1.size()>,<value1>,<value2.size()>,...))

      char log_type = line[1];
      DebugAssert(log_type == 'c' || log_type == 'i' || log_type == 'v' || log_type == 'l',
                  "Recoverer: invalid log type token");

      // next_token_begin is set to the pos of the next token after each read
      // the first token after the log type identifier is at position 3
      size_t next_token_begin = 3;

      // if commit entry
      if (log_type == 'c') {
        TransactionID transaction_id = std::stoul(_extract_token_up_to_delimiter(line, next_token_begin, ')'));
        _redo_transaction(transactions, transaction_id);
        continue;
      }

      // if load table entry
      if (log_type == 'l') {
        std::string path = _extract_next_value_with_preceding_size(line, next_token_begin, ',', log_file);
        std::string table_name = _extract_next_value_with_preceding_size(line, next_token_begin, ',', log_file);
        DebugAssert(line[next_token_begin - 1] == ')',
                    "Recoverer: load table entry expected ')', but got " + line[next_token_begin - 1] + " instead.");

        _redo_load_table(path, table_name);
        continue;
      }

      // else: value or invalidation entry

      TransactionID transaction_id = std::stoul(_extract_token_up_to_delimiter(line, next_token_begin, ','));

      std::string table_name = _extract_next_value_with_preceding_size(line, next_token_begin, ',', log_file);

      // <RowID> = RowID(<chunk_id>,<chunk_offset>)
      // "RowID(".length() = 6
      next_token_begin += 6;
      ChunkID chunk_id(std::stoul(_extract_token_up_to_delimiter(line, next_token_begin, ',')));
      ChunkOffset chunk_offset(std::stoul(_extract_token_up_to_delimiter(line, next_token_begin, ')')));
      RowID row_id{chunk_id, chunk_offset};

      // if invalidation
      if (log_type == 'i') {
        transactions[transaction_id].emplace_back(
            std::make_unique<LoggedInvalidation>(LoggedInvalidation(transaction_id, table_name, row_id)));
        continue;
      }

      // else: value entry
      // (<value1.size()>,<value1>,<value2.size()>,...)

      // adding two braces of "chunk_offset_end),(value_size_begin"
      next_token_begin += 2;

      DebugAssert(line[next_token_begin - 2] == ',',
                  "Recoverer: Expected ',' but got '" + line[next_token_begin - 2] + "' instead")
          DebugAssert(line[next_token_begin - 1] == '(',
                      "Recoverer: Expected '(' but got '" + line[next_token_begin - 1] + "' instead")

              std::vector<AllTypeVariant>
                  values;
      // while still values in line
      while (line[next_token_begin - 1] != ')') {
        values.emplace_back(_extract_next_value_with_preceding_size(line, next_token_begin, ',', log_file));
        DebugAssert(line.length() >= next_token_begin, "Recoverer: line ended before ')'");
      }

      transactions[transaction_id].emplace_back(
          std::make_unique<LoggedValue>(LoggedValue(transaction_id, table_name, row_id, values)));
    }
  }

  return _number_of_loaded_tables;
}

}  // namespace opossum
