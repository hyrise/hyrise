#pragma once

#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

class AbstractLogger {
 public:
  AbstractLogger(const AbstractLogger&) = delete;
  AbstractLogger& operator=(const AbstractLogger&) = delete;

  // the implementation needs to call callback(transaction_id)
  virtual void commit(const TransactionID transaction_id, std::function<void(TransactionID)> callback) = 0;

  virtual void value(const TransactionID transaction_id, const std::string table_name, const RowID row_id,
                     const std::vector<AllTypeVariant> values) = 0;

  virtual void invalidate(const TransactionID transaction_id, const std::string table_name, const RowID row_id) = 0;

  virtual void flush() = 0;

  virtual void recover() = 0;

  virtual ~AbstractLogger() = default;

protected:
  AbstractLogger(){}

  // functions to receive and set log file numbers
  // received log number has to be set after file creation
  u_int32_t _get_new_log_number();
  void _set_last_log_number(u_int32_t log_number);

};

}  // namespace opossum
