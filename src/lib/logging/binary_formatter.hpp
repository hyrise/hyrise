#pragma once

#include "abstract_formatter.hpp"

#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

class BinaryFormatter : public AbstractFormatter {
 public:
  std::vector<char> commit_entry(const TransactionID transaction_id) final;

  std::vector<char> value_entry(const TransactionID transaction_id, const std::string& table_name, 
                                       const RowID row_id, const std::vector<AllTypeVariant>& values) final;

  std::vector<char> invalidate_entry(const TransactionID transaction_id, const std::string& table_name,
                                            const RowID row_id) final;

  std::vector<char> load_table_entry(const std::string& file_path, const std::string& table_name) final;
};

}  // namespace opossum
