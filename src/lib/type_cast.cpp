#include "type_cast.hpp"

#include <string>

namespace opossum {

std::string to_string(const AllTypeVariant& x) { return boost::lexical_cast<std::string>(x); }

}  // namespace opossum
