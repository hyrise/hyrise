#include "all_type_variant.hpp"

#include <cstddef>
#include <functional>
#include <ostream>

#include <boost/variant/detail/hash_variant.hpp>

#include "types.hpp"

namespace hyrise {

bool is_floating_point_data_type(const DataType data_type) {
  return data_type == DataType::Float || data_type == DataType::Double;
}

std::ostream& operator<<(std::ostream& stream, const DataType data_type) {
  return stream << data_type_to_string.left.at(data_type);
}

}  // namespace hyrise

namespace std {

size_t hash<hyrise::AllTypeVariant>::operator()(const hyrise::AllTypeVariant& all_type_variant) const {
  return boost::hash_value(all_type_variant);
}

}  // namespace std
