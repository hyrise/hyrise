#pragma once

#include <fstream>
#include <mutex>

#include "abstract_formatter.hpp"
#include "abstract_logger.hpp"
#include "types.hpp"

namespace opossum {

/*
 *  Logger that is implemented in a naive way and writes entries into a text file.
 */
class SimpleLogger final: public AbstractLogger {
 public:
  SimpleLogger(const SimpleLogger&) = delete;
  SimpleLogger& operator=(const SimpleLogger&) = delete;

  void log_commit(const TransactionID transaction_id, std::function<void(TransactionID)> callback) final;

  void log_value(const TransactionID transaction_id, const std::string& table_name, const RowID row_id,
                 const std::vector<AllTypeVariant>& values) final;

  void log_invalidate(const TransactionID transaction_id, const std::string& table_name, const RowID row_id) final;

  void log_load_table(const std::string& file_path, const std::string& table_name) final;

  void log_flush() final;

 protected:
  void _write_to_logfile(const std::vector<char> data);

 private:
  friend class Logger;
  explicit SimpleLogger(std::unique_ptr<AbstractFormatter> formatter);
};

}  // namespace opossum
