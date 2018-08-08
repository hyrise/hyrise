#pragma once

#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

class AbstractLogger {
 public:
  AbstractLogger(const AbstractLogger&) = delete;
  AbstractLogger& operator=(const AbstractLogger&) = delete;

  // A transaction is committed only after calling its callback.
  // Therefore handle all callbacks in your Logger implementation.
  virtual void commit(const TransactionID transaction_id, std::function<void(TransactionID)> callback) = 0;

  virtual void value(const TransactionID transaction_id, const std::string& table_name, const RowID row_id,
                     const std::vector<AllTypeVariant> values) = 0;

  virtual void invalidate(const TransactionID transaction_id, const std::string& table_name, const RowID row_id) = 0;

  // this method should always call flush(), since load table does not commit
  virtual void load_table(const std::string& file_path, const std::string& table_name) = 0;

  virtual void flush() = 0;

  virtual void recover() = 0;

  virtual ~AbstractLogger() = default;

 protected:
  friend class Logger;

  AbstractLogger() {}
}

}  // namespace opossum
