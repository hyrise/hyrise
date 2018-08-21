/*  
 *  The SimpleLogger naively logs entries in a text file.
 * 
 *  
 *  The log entries have following format:
 * 
 *     Commit Entries:
 *      (t,<TransactionID>)\n
 * 
 *     Value Entries:
 *      (v,<TransactionID>,<table_name.size()>,<table_name>,<RowID>,(<value1.size()>,<value1>,<value2.size()>,...))\n
 * 
 *     Invalidation Entries:
 *      (i,<TransactionID>,<table_name.size()>,<table_name>,<RowID>)\n
 * 
 *     Load Table Entries:
 *      (l,<path.size()>,<path>,<table_name.size()>,<table_name>)\n
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
  const auto& data = _formatter->invalidate_entry(transaction_id, table_name, row_id);
  _write_to_logfile(data);
}

void SimpleLogger::log_load_table(const std::string& file_path, const std::string& table_name) {
  const auto& data = _formatter->load_table_entry(file_path, table_name);
  _write_to_logfile(data);
  log_flush();
}

void SimpleLogger::log_flush() { fsync(_file_descriptor); }

void SimpleLogger::_write_to_logfile(const std::vector<char> data) {
  _file_mutex.lock();
  DebugAssert(_file_descriptor != -1, "Logger: Logfile not open");
  write(_file_descriptor, reinterpret_cast<const void*>(&data[0]), data.size());
  _file_mutex.unlock();
}

void SimpleLogger::_open_logfile() {
  _file_mutex.lock();

  auto path = Logger::get_new_log_path();

  // read and write rights needed, since default rights do not allow to reopen the file after restarting the db
  mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
  int oflags = O_WRONLY | O_APPEND | O_CREAT;

  _file_descriptor = open(path.c_str(), oflags, mode);

  DebugAssert(_file_descriptor != -1, "Logger: Logfile could not be opened or created: " + path);

  _file_mutex.unlock();
}

SimpleLogger::SimpleLogger(std::unique_ptr<AbstractFormatter> formatter) : AbstractLogger(std::move(formatter)) { _open_logfile(); }

}  // namespace opossum
