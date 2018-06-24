#pragma once

#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

class AbstractRecovery {
 public:
  AbstractRecovery(const AbstractRecovery&) = delete;
  AbstractRecovery& operator=(const AbstractRecovery&) = delete;

  virtual void recover() = 0;

 protected:
  AbstractRecovery(){}

  enum class LogType { Value, Invalidation };

  class LoggedItem {
  public:
    LoggedItem(LogType type, TransactionID& transaction_id, std::string& table_name, RowID& row_id,
              std::vector<AllTypeVariant>& values)
    : type(type), transaction_id(transaction_id), table_name(table_name), row_id(row_id), values(values) {
      DebugAssert(type == LogType::Value, "called value LoggedItem with wrong type");
    }

    LoggedItem(LogType type, TransactionID& transaction_id, std::string& table_name, RowID& row_id)
        : type(type), transaction_id(transaction_id), table_name(table_name), row_id(row_id) {
      DebugAssert(type == LogType::Invalidation, "called invalidation LoggedItem with wrong type");
    }

    LogType type;
    TransactionID transaction_id;
    std::string table_name;
    RowID row_id;
    std::optional<std::vector<AllTypeVariant>> values;
  };

  void _redo_transactions(const TransactionID& transaction_id, std::vector<LoggedItem>& transactions);
};

}  // namespace opossum
