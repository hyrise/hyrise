#include "all_type_variant.hpp" // NEEDEDINCLUDE

#include <boost/functional/hash/hash.hpp>

namespace opossum {

bool is_floating_point_data_type(const DataType data_type) {
  return data_type == DataType::Float || data_type == DataType::Double;
}

}  // namespace opossum

namespace std {

size_t hash<opossum::AllTypeVariant>::operator()(const opossum::AllTypeVariant& all_type_variant) const {
  return boost::hash_value(all_type_variant);
}

}  // namespace std
