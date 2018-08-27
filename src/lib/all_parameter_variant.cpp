#include "all_parameter_variant.hpp"

#include <sstream>
#include <string>

#include "boost/lexical_cast.hpp"

#include "all_type_variant.hpp"

namespace opossum {
std::string to_string(const AllParameterVariant& x) {
  if (is_parameter_id(x)) {
    return std::string("Placeholder #") + std::to_string(boost::get<ParameterID>(x));
  } else if (is_column_id(x)) {
    return std::string("Col #") + std::to_string(boost::get<ColumnID>(x));
  } else if (is_lqp_column_reference(x)) {
    std::stringstream stream;
    stream << boost::get<LQPColumnReference>(x);
    return stream.str();
  } else {
    return boost::lexical_cast<std::string>(x);
  }
}

}  // namespace opossum
