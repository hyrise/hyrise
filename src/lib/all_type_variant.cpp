#include "all_type_variant.hpp"

namespace opossum {

template<>
DataType data_type<bool>() {
  return DataType::Bool;
}

template<>
DataType data_type<int32_t>() {
  return DataType::Int;
}

template<>
DataType data_type<int64_t>() {
  return DataType::Long;
}

template<>
DataType data_type<float>() {
  return DataType::Float;
}

template<>
DataType data_type<double>() {
  return DataType::Double;
}

template<>
DataType data_type<std::string>() {
  return DataType::String;
}

}
