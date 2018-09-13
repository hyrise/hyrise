#pragma once

#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

/*  
 *  AbstractRecoverer is the abstract class for all recovery implementations. 
 *  It serves functionality that is used by all recoverers, namely 
 *   -  recovery from a file
 *   -  replay a transaction
 *   -  update transaction_id in the TransactionManager
 * 
 *  Furthermore it defines LogType and LoggedItem that are used during recovery.
 */

class AbstractRecoverer {
 public:
  AbstractRecoverer(const AbstractRecoverer&) = delete;
  AbstractRecoverer& operator=(const AbstractRecoverer&) = delete;

  // Recovers db from logfiles and returns the number of loaded tables
  virtual uint32_t recover() = 0;

 protected:
  AbstractRecoverer() {}

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

  void _redo_transaction(std::map<TransactionID, std::vector<LoggedItem>>& transactions, TransactionID transaction_id);

  void _redo_load_table(const std::string& path, const std::string& table_name);

  uint32_t _number_of_loaded_tables;
};

}  // namespace opossum
