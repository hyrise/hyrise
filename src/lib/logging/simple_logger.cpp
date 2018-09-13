/*  
 *  The SimpleLogger naively logs entries into a file. 
 *  It just writes everything directly to disk and flushes on each commit and table load.
 */

#include "simple_logger.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sstream>

#include "all_type_variant.hpp"
#include "logger.hpp"
#include "text_recoverer.hpp"

namespace opossum {

void SimpleLogger::log_commit(const TransactionID transaction_id, std::function<void(TransactionID)> callback) {
  const auto& data = _formatter->commit_entry(transaction_id);
  _write_to_logfile(data);
  log_flush();
  callback(transaction_id);
}

void SimpleLogger::log_value(const TransactionID transaction_id, const std::string& table_name, const RowID row_id,
                             const std::vector<AllTypeVariant>& values) {
  const auto& data = _formatter->value_entry(transaction_id, table_name, row_id, values);
  _write_to_logfile(data);
}

void SimpleLogger::log_invalidate(const TransactionID transaction_id, const std::string& table_name,
                                  const RowID row_id) {
  const auto& data = _formatter->create_invalidation_entry(transaction_id, table_name, row_id);
  _write_to_logfile(data);
}

void SimpleLogger::log_load_table(const std::string& file_path, const std::string& table_name) {
  const auto& data = _formatter->load_table_entry(file_path, table_name);
  _write_to_logfile(data);
  log_flush();
}

void SimpleLogger::log_flush() {
  {
    std::scoped_lock file_lock(_file_mutex);
    _log_file.sync();
  }
}

void SimpleLogger::_write_to_logfile(const std::vector<char> data) {
  {
    std::scoped_lock file_lock(_file_mutex);
    DebugAssert(_log_file.is_open(), "Logger: Log file not open.");
    _log_file.write(&data[0], data.size());
  }
}

SimpleLogger::SimpleLogger(std::unique_ptr<AbstractLogFormatter> formatter) : AbstractLogger(std::move(formatter)) {
  _open_logfile();
}

}  // namespace opossum
