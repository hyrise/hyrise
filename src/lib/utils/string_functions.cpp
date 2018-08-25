#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim_all.hpp>

#include "string_functions.hpp"

namespace opossum {

std::vector<std::string> trim_and_split(const std::string& input) {
  std::string converted = input;

  boost::algorithm::trim_all<std::string>(converted);
  std::vector<std::string> arguments;
  boost::algorithm::split(arguments, converted, boost::is_space());

  return arguments;
}

}  // namespace opossum
