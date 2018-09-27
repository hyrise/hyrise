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

const std::string plugin_name_from_path(const filesystem::path& path) {
  const auto filename = path.stem().string();

  // Remove "lib" prefix of shared library file
  const auto plugin_name = filename.substr(3);

  return plugin_name;
}

}  // namespace opossum
