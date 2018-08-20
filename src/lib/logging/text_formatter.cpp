#include "text_formatter.hpp"

#include <sys/types.h>
#include <sstream>

#include "all_type_variant.hpp"

namespace opossum{

std::vector<char> TextFormatter::_char_vector_of(std::stringstream& ss) {
  auto data_string = ss.str();
  return std::vector<char>(data_string.begin(), data_string.end());
}

std::vector<char> TextFormatter::commit_entry(const TransactionID transaction_id) {
  std::stringstream ss;
  ss << "(t," << transaction_id << ")\n";

  return _char_vector_of(ss);
}

std::vector<char> TextFormatter::value_entry(const TransactionID transaction_id, const std::string& table_name, 
                                              const RowID row_id, const std::vector<AllTypeVariant>& values) {
  std::stringstream ss;
  ss << "(v," << transaction_id << "," << table_name.size() << "," << table_name << "," << row_id << ",(";

  std::stringstream value_ss;
  value_ss << values[0];
  ss << value_ss.str().size() << "," << value_ss.str();
  for (auto value = ++values.begin(); value != values.end(); ++value) {
    value_ss.str("");
    value_ss << (*value);
    ss << "," << value_ss.str().size() << "," << value_ss.str();
  }

  ss << "))\n";

  return _char_vector_of(ss);
}

std::vector<char> TextFormatter::invalidate_entry(const TransactionID transaction_id, const std::string& table_name,
                                                   const RowID row_id) {
  std::stringstream ss;
  ss << "(i," << transaction_id << "," << table_name.size() << "," << table_name << "," << row_id << ")\n";

  return _char_vector_of(ss);
}

std::vector<char> TextFormatter::load_table_entry(const std::string& file_path, const std::string& table_name) {
  std::stringstream ss;
  ss << "(l," << file_path.size() << "," << file_path << "," << table_name.size() << "," << table_name << ")\n";

  return _char_vector_of(ss);
}


}  // namespace opossum
