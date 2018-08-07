#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>

#include <string>

namespace opossum {

std::vector<std::string> trim_and_split(const std::string &input) {
  std::string converted = input;
  
  boost::algorithm::trim<std::string>(converted);
  std::vector<std::string> arguments;
  boost::algorithm::split(arguments, input, boost::is_space());

  return arguments;
}


}  // namespace opossum
