#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim_all.hpp>

#include "string_utils.hpp"

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
  const auto plugin_name = filename.substr(3);

  return plugin_name;
}

std::string trim_source_file_path(const std::string& path) {
  const auto src_pos = path.find("/src/");
  if (src_pos == std::string::npos) return path;

  // "+ 1", since we want "src/lib/file.cpp" and not "/src/lib/file.cpp"
  return path.substr(src_pos + 1);
}

}  // namespace opossum
