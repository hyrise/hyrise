#pragma once

#include "abstract_logger.hpp"

#include "types.hpp"

namespace opossum {

class InitialLogger : public AbstractLogger{
 public:  
  InitialLogger(const InitialLogger&) = delete;
  InitialLogger& operator=(const InitialLogger&) = delete;

  void commit(const TransactionID transaction_id, std::function<void(TransactionID)> callback) override;

  void value(const TransactionID transaction_id, const std::string table_name, const RowID row_id, const std::vector<AllTypeVariant> values) override;

  void invalidate(const TransactionID transaction_id, const std::string table_name, const RowID row_id) override;

  // void flush() override;

 private:
  friend class Logger;
  InitialLogger();
};

}  // namespace opossum
