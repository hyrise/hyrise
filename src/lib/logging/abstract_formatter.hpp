#pragma once

#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

class AbstractFormatter {
 public:
  virtual std::vector<char> commit_entry(const TransactionID transaction_id) = 0;

  virtual std::vector<char> value_entry(const TransactionID transaction_id, const std::string& table_name, 
                                               const RowID row_id, const std::vector<AllTypeVariant>& values) = 0;

  virtual std::vector<char> invalidate_entry(const TransactionID transaction_id, const std::string& table_name,
                                                    const RowID row_id) = 0;

  virtual std::vector<char> load_table_entry(const std::string& file_path, const std::string& table_name) = 0;

  AbstractFormatter() = default;
  virtual ~AbstractFormatter() = default;
};

}  // namespace opossum