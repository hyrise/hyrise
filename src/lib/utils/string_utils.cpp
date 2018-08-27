#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim_all.hpp>

#include "string_utils.hpp"

namespace opossum {

// Removes whitspaces from the front and back. Also reduces multiple whitespaces between words to a single one.
// Splits the result by whitespace into single words. Intended for usage in the console.
std::vector<std::string> trim_and_split(const std::string& input) {
  std::string converted = input;

  boost::algorithm::trim_all<std::string>(converted);
  std::vector<std::string> arguments;
  boost::algorithm::split(arguments, converted, boost::is_space());

  return arguments;
}

}  // namespace opossum
