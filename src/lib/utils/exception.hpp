#include <iostream>
#include <exception>
#include <stdexcept>
#include <sstream>

namespace opossum {

class InvalidInput : public std::runtime_error {
 public:
  explicit InvalidInput(const std::string& what_arg) : std::runtime_error(what_arg){}
};


}
