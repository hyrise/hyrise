#include "all_type_variant.hpp"

#include <cmath>

#include <boost/container_hash/hash.hpp>

#include "utils/assert.hpp"

namespace hyrise {

bool is_floating_point_data_type(const DataType data_type) {
  return data_type == DataType::Float || data_type == DataType::Double;
}

}  // namespace hyrise

namespace std {

size_t hash<hyrise::AllTypeVariant>::operator()(const hyrise::AllTypeVariant& all_type_variant) const {
  return boost::hash_value(all_type_variant);
}

}  // namespace std
