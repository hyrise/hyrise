#include "all_parameter_variant.hpp"

#include <boost/lexical_cast.hpp>
#include <string>

#include "all_type_variant.hpp"

namespace opossum {
std::string to_string(const AllParameterVariant& x) {
  if (is_placeholder(x)) {
    return std::string("Placeholder #") + std::to_string(boost::get<ValuePlaceholder>(x).index());
  } else if (is_column_id(x)) {
    return std::string("Col #") + std::to_string(boost::get<ColumnID>(x));
  } else if (is_lqp_column_reference(x)) {
    return boost::get<LQPColumnReference>(x).description();
  } else {
    return boost::lexical_cast<std::string>(x);
  }
}

bool all_parameter_variant_near(const AllParameterVariant& lhs, const AllParameterVariant& rhs, double max_abs_error) {
  if (lhs.type() == typeid(AllTypeVariant) && rhs.type() == typeid(AllTypeVariant)) {
    return all_type_variant_near(boost::get<AllTypeVariant>(lhs), boost::get<AllTypeVariant>(rhs), max_abs_error);
  }

  return lhs == rhs;
}
}  // namespace opossum
