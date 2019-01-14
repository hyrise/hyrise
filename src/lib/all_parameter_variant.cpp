#include "all_parameter_variant.hpp" // NEEDEDINCLUDE


#include "boost/lexical_cast.hpp" // NEEDEDINCLUDE


namespace opossum {
std::string to_string(const AllParameterVariant& x) {
  if (is_parameter_id(x)) {
    return std::string("Placeholder #") + std::to_string(boost::get<ParameterID>(x));
  } else if (is_column_id(x)) {
    return std::string("Column #") + std::to_string(boost::get<ColumnID>(x));
  } else {
    return boost::lexical_cast<std::string>(x);
  }
}

}  // namespace opossum
