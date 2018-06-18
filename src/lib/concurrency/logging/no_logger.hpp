/*
 *  Used to turn logging off.
 *  NoLogger does not log anything and just calls the commit callback of corresponding transactions.
 */

#pragma once

#include "abstract_logger.hpp"

#include "types.hpp"

namespace opossum {

class NoLogger : public AbstractLogger {
 public:
  NoLogger(const NoLogger&) = delete;
  NoLogger& operator=(const NoLogger&) = delete;

  void commit(const TransactionID transaction_id, std::function<void(TransactionID)> callback) override {
    callback(transaction_id);
  };

  void value(const TransactionID transaction_id, const std::string table_name, const RowID row_id,
             const std::vector<AllTypeVariant> values) override {};

  void invalidate(const TransactionID transaction_id, const std::string table_name, const RowID row_id) override {};

  void flush() override {};

  void recover() override {};

 private:
  friend class Logger;
  NoLogger() = default;
};

}  // namespace opossum
