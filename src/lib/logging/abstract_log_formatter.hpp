#pragma once

#include "abstract_recoverer.hpp"
#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

// AbstractLogFormatter is the abstract class for all log format implementations.
// It serves the interface to create log entries for all db transactions and recover from logfiles on startup.

class AbstractLogFormatter {
 public:
  // Creates a commit entry
  virtual std::vector<char> create_commit_entry(const TransactionID transaction_id) = 0;

  // Creates a value entry
  virtual std::vector<char> create_value_entry(const TransactionID transaction_id, const std::string& table_name,
                                        const RowID row_id, const std::vector<AllTypeVariant>& values) = 0;

  // Creates a invalidation entry
  virtual std::vector<char> create_invalidation_entry(const TransactionID transaction_id, const std::string& table_name,
                                             const RowID row_id) = 0;

  // Creates a load table entry
  virtual std::vector<char> create_load_table_entry(const std::string& file_path, const std::string& table_name) = 0;

  // Returns the corresponding recoverer
  virtual AbstractRecoverer& get_recoverer() = 0;

  AbstractLogFormatter() = default;
  virtual ~AbstractLogFormatter() = default;
};

}  // namespace opossum
