#pragma once

#include "abstract_logger.hpp"

#include "types.hpp"

namespace opossum {

/*
 *  Logger that is implemented in a naive way and writes entries into a text file.
 */
class SimpleLogger : public AbstractLogger {
 public:
  SimpleLogger(const SimpleLogger&) = delete;
  SimpleLogger& operator=(const SimpleLogger&) = delete;

  void commit(const TransactionID transaction_id, std::function<void(TransactionID)> callback) override;

  void value(const TransactionID transaction_id, const std::string table_name, const RowID row_id,
             const std::vector<AllTypeVariant> values) override;

  void invalidate(const TransactionID transaction_id, const std::string table_name, const RowID row_id) override;

  void flush() override;

  void recover() override;

 private:
  friend class Logger;
  SimpleLogger();

 private:
  void _write_to_logfile(const std::stringstream& ss);

  int _file_descriptor;
  std::mutex _file_mutex;
};

}  // namespace opossum
