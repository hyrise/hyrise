#pragma once

#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

class TextFormatter {
 public:
  static std::vector<char> commit_entry(const TransactionID transaction_id);

  static std::vector<char> value_entry(const TransactionID transaction_id, const std::string& table_name, 
                                       const RowID row_id, const std::vector<AllTypeVariant>& values);

  static std::vector<char> invalidate_entry(const TransactionID transaction_id, const std::string& table_name,
                                            const RowID row_id);

  static std::vector<char> load_table_entry(const std::string& file_path, const std::string& table_name);

 protected:
  static std::vector<char> _char_vector_of(std::stringstream& ss);
};

}  // namespace opossum
