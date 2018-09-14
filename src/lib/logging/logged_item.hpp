#pragma once

#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

/*
 *  Logged Items are created during recovery.
 *  If a transaction commit is read from logfile,
 *  .redo() is called on the corresponding Logged Items
 */

class LoggedItem {
 public:
  virtual void redo() = 0;

  virtual ~LoggedItem() = default;

 protected:
  LoggedItem(TransactionID& transaction_id, std::string& table_name, RowID& row_id);

  TransactionID _transaction_id;
  std::string _table_name;
  RowID _row_id;
};

class LoggedInvalidation final : public LoggedItem {
 public:
  LoggedInvalidation(TransactionID& transaction_id, std::string& table_name, RowID& row_id);
  void redo() final;
};

class LoggedValue final: public LoggedItem {
 public:
  LoggedValue(TransactionID& transaction_id, std::string& table_name, RowID& row_id, 
              std::vector<AllTypeVariant>& values);
  void redo() final;

 protected:
  std::vector<AllTypeVariant> _values;
};


}  // namespace opossum
