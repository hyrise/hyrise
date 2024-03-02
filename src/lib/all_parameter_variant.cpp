#include "all_parameter_variant.hpp"

#include <ostream>

#include <boost/variant/get.hpp>

#include "all_type_variant.hpp"
#include "types.hpp"

namespace hyrise {

std::ostream& operator<<(std::ostream& stream, const AllParameterVariant& variant) {
  if (is_parameter_id(variant)) {
    stream << "Placeholder #" << boost::get<ParameterID>(variant);
  } else if (is_column_id(variant)) {
    stream << "Column #" << boost::get<ColumnID>(variant);
  } else {
    stream << boost::get<AllTypeVariant>(variant);
  }

  return stream;
}

}  // namespace hyrise
