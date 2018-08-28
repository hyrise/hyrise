#include "all_parameter_variant.hpp"

#include <sstream>
#include <string>

#include "boost/lexical_cast.hpp"

#include "all_type_variant.hpp"

namespace opossum {
std::string to_string(const AllParameterVariant& x) {
  if (is_parameter_id(x)) {
    return std::string("Placeholder #") + std::to_string(boost::get<ParameterID>(x));
  } else if (is_cxlumn_id(x)) {
    return std::string("Cxlumn #") + std::to_string(boost::get<CxlumnID>(x));
  } else if (is_lqp_cxlumn_reference(x)) {
    std::stringstream stream;
    stream << boost::get<LQPCxlumnReference>(x);
    return stream.str();
  } else {
    return boost::lexical_cast<std::string>(x);
  }
}

}  // namespace opossum
