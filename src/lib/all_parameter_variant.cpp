#include "all_parameter_variant.hpp"

#include <string>

namespace opossum {
std::string to_string(const AllParameterVariant& x) {
  if (is_placeholder(x)) {
    return std::string("Placeholder #") + std::to_string(boost::get<ValuePlaceholder>(x).index());
  } else if (is_column_id(x)) {
    return std::string("Col #") + std::to_string(boost::get<ColumnID>(x));
  } else {
    return boost::lexical_cast<std::string>(x);
  }
}
}  // namespace opossum
