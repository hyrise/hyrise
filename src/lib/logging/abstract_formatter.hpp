#pragma once

#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

// AbstractFormatter is the abstract class for all log format implementations.
// It serves the interface to create log entries for all db transactions and recover from logfiles on startup.

class AbstractFormatter {
 public:
  // Creates a commit entry
  virtual std::vector<char> commit_entry(const TransactionID transaction_id) = 0;

  // Creates a value entry
  virtual std::vector<char> value_entry(const TransactionID transaction_id, const std::string& table_name,
                                        const RowID row_id, const std::vector<AllTypeVariant>& values) = 0;

  // Creates a invalidation entry
  virtual std::vector<char> invalidate_entry(const TransactionID transaction_id, const std::string& table_name,
                                             const RowID row_id) = 0;

  // Creates a load table entry
  virtual std::vector<char> load_table_entry(const std::string& file_path, const std::string& table_name) = 0;

  // Calls the corresponding recoverer
  virtual uint32_t recover() = 0;

  AbstractFormatter() = default;
  virtual ~AbstractFormatter() = default;
};

}  // namespace opossum