#include "type_cast.hpp"

namespace opossum {

std::string to_string(const AllTypeVariant& x) { return boost::lexical_cast<std::string>(x); }

}  // namespace opossum
