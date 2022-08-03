#include "string_utils.hpp"

#include <regex>

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim_all.hpp>
#include <magic_enum.hpp>

#include "storage/table_key_constraint.hpp"

namespace opossum {

std::vector<std::string> trim_and_split(const std::string& input) {
  std::string converted = input;

  boost::algorithm::trim_all<std::string>(converted);
  std::vector<std::string> arguments;
  boost::algorithm::split(arguments, converted, boost::is_space());

  return arguments;
}

std::vector<std::string> split_string_by_delimiter(const std::string& str, char delimiter) {
  std::vector<std::string> internal;
  std::stringstream ss(str);
  std::string tok;

  while (std::getline(ss, tok, delimiter)) {
    internal.push_back(tok);
  }

  return internal;
}

std::string plugin_name_from_path(const std::filesystem::path& path) {
  const auto filename = path.stem().string();

  // Remove "lib" prefix of shared library file
  auto plugin_name = filename.substr(3);

  return plugin_name;
}

std::string trim_source_file_path(const std::string& path) {
  const auto src_pos = path.find("/src/");
  if (src_pos == std::string::npos) {
    return path;
  }

  // "+ 1", since we want "src/lib/file.cpp" and not "/src/lib/file.cpp"
  return path.substr(src_pos + 1);
}

std::string replace_addresses(const std::string& input) {
  return std::regex_replace(input, std::regex{"0x[0-9A-Fa-f]{4,}"}, "0x00000000");
}

void table_key_constraints_to_stream(std::ostream& stream, const std::shared_ptr<const Table>& table,
                                     const std::string& separator) {
  const auto& table_key_constraints = table->soft_key_constraints();
  for (auto constraint_it = table_key_constraints.cbegin(); constraint_it != table_key_constraints.cend();
       ++constraint_it) {
    const auto& table_key_constraint = *constraint_it;
    stream << magic_enum::enum_name(table_key_constraint.key_type()) << "(";
    const auto& columns = table_key_constraint.columns();
    for (auto column_it = columns.cbegin(); column_it != columns.cend(); ++column_it) {
      stream << table->column_name(*column_it);
      if (std::next(column_it) != columns.cend()) {
        stream << ", ";
      }
    }
    stream << ")";
    if (std::next(constraint_it) != table_key_constraints.cend()) {
      stream << separator;
    }
  }
}

}  // namespace opossum
