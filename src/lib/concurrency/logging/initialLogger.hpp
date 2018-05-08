#pragma once

#include "types.hpp"

namespace opossum {

class InitialLogger {
 public:  
  InitialLogger(const InitialLogger&) = delete;
  InitialLogger& operator=(const InitialLogger&) = delete;

  static InitialLogger& getInstance();

  void log_commit(const TransactionID transaction_id);

  void log_value(const TransactionID transaction_id, const std::string table_name, const RowID row_id, const std::stringstream &values);

  void invalidate(const TransactionID transaction_id, const std::string table_name, const RowID row_id);

  void flush();

 private:
  InitialLogger();

  int _file_descriptor;
  std::mutex _mutex;
};

}  // namespace opossum
