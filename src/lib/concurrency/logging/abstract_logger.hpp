#pragma once

#include "types.hpp"
#include "all_type_variant.hpp"

namespace opossum {

/*
  Existence of logfile is only checked on instantiation. Therefore deleting the log during runtime is undetected.
*/

class AbstractLogger {
 public:  
  AbstractLogger(const AbstractLogger&) = delete;
  AbstractLogger& operator=(const AbstractLogger&) = delete;

  virtual void commit(const TransactionID transaction_id) = 0;

  virtual void value(const TransactionID transaction_id, const std::string table_name, const RowID row_id, const std::vector<AllTypeVariant> values) = 0;

  virtual void invalidate(const TransactionID transaction_id, const std::string table_name, const RowID row_id) = 0;

  virtual void flush();

  virtual ~AbstractLogger() = default;

 protected:
  virtual void _write_to_logfile(const std::stringstream& ss);

  int _file_descriptor;
  std::mutex _file_mutex;

 private:
  friend class Logger;
  friend class InitialLogger;
  friend class GroupCommitLogger;
  AbstractLogger();
};

}  // namespace opossum