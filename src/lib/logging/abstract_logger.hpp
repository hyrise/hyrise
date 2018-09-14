#pragma once

#include <fstream>
#include <mutex>

#include "abstract_log_formatter.hpp"
#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

// AbstractLogger is the abstract class for all logging implementations.
// It serves the interface to log all db transactions and recover from logfiles on startup.

class AbstractLogger {
 public:
  AbstractLogger(const AbstractLogger&) = delete;
  AbstractLogger& operator=(const AbstractLogger&) = delete;

  // Logs the commit of a transaction.
  // A transaction is committed only after calling its callback.
  // Therefore handle all callbacks in your Logger implementation.
  virtual void log_commit(const TransactionID transaction_id, std::function<void(TransactionID)> callback) = 0;

  // Log a single row. Used in case of insert and update.
  virtual void log_value(const TransactionID transaction_id, const std::string& table_name, const RowID row_id,
                         const std::vector<AllTypeVariant>& values) = 0;

  // Log an invalidation. Used in case of delete and update.
  virtual void log_invalidation(const TransactionID transaction_id, const std::string& table_name,
                                const RowID row_id) = 0;

  // Log the load table command.
  // This method should always call log_flush(), since load table does not commit.
  virtual void log_load_table(const std::string& file_path, const std::string& table_name) = 0;

  // Flushes log to disk.
  virtual void log_flush() = 0;

  // Returns the corresponding recoverer
  virtual AbstractRecoverer& get_recoverer() { return _formatter->get_recoverer(); }

  virtual ~AbstractLogger() = default;

 protected:
  friend class Logger;
  explicit AbstractLogger(std::unique_ptr<AbstractLogFormatter> formatter);

 protected:
  void _open_logfile();

  std::unique_ptr<AbstractLogFormatter> _formatter;
  std::mutex _file_mutex;
  std::fstream _log_file;
};

}  // namespace opossum
