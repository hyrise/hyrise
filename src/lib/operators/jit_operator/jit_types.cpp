#include "jit_types.hpp"

namespace opossum {

template <>
bool JitVariantVector::get(const size_t index) const {
  return _bool[index];
}

template <>
int32_t JitVariantVector::get(const size_t index) const {
  return _int[index];
}

template <>
int64_t JitVariantVector::get(const size_t index) const {
  return _long[index];
}

template <>
float JitVariantVector::get(const size_t index) const {
  return _float[index];
}

template <>
double JitVariantVector::get(const size_t index) const {
  return _double[index];
}

template <>
std::string JitVariantVector::get(const size_t index) const {
  return _string[index];
}

template <>
void JitVariantVector::set(const size_t index, const bool value) {
  _bool[index] = value;
}

template <>
void  JitVariantVector::set(const size_t index, const int32_t value) {
  _int[index] = value;
}

template <>
void  JitVariantVector::set(const size_t index, const int64_t value) {
  _long[index] = value;
}

template <>
void  JitVariantVector::set(const size_t index, const float value) {
  _float[index] = value;
}

template <>
void  JitVariantVector::set(const size_t index, const double value) {
  _double[index] = value;
}

template <>
void JitVariantVector::set(const size_t index, const std::string value) {
  _string[index] = value;
}

}  // namespace opossum
