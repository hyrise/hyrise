#include "jit_types.hpp"

namespace opossum {

template <>
JitDataType jit_data_type<uint8_t>() {
  return JitDataType::Bool;
}

template <>
JitDataType jit_data_type<int32_t>() {
  return JitDataType::Int;
}

template <>
JitDataType jit_data_type<int64_t>() {
  return JitDataType::Long;
}

template <>
JitDataType jit_data_type<float>() {
  return JitDataType::Float;
}

template <>
JitDataType jit_data_type<double>() {
  return JitDataType::Double;
}

template <>
JitDataType jit_data_type<std::string>() {
  return JitDataType::String;
}

template <>
uint8_t& JitVariantVector::as(const size_t index) {
  return _bool[index];
}

template <>
int32_t& JitVariantVector::as(const size_t index) {
  return _int[index];
}

template <>
int64_t& JitVariantVector::as(const size_t index) {
  return _long[index];
}

template <>
float& JitVariantVector::as(const size_t index) {
  return _float[index];
}

template <>
double& JitVariantVector::as(const size_t index) {
  return _double[index];
}

template <>
std::string& JitVariantVector::as(const size_t index) {
  return _string[index];
}

}  // namespace opossum
