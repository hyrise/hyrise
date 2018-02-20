#include "jit_types.hpp"

namespace opossum {

#define JIT_VARIANT_VECTOR_GET(r, d, type)          \
  template <>                                       \
  BOOST_PP_TUPLE_ELEM(3, 0, type)                   \
  JitVariantVector::get(const size_t index) const { \
    return BOOST_PP_TUPLE_ELEM(3, 1, type)[index];  \
  }

#define JIT_VARIANT_VECTOR_SET(r, d, type)                                                      \
  template <>                                                                                   \
  void JitVariantVector::set(const size_t index, const BOOST_PP_TUPLE_ELEM(3, 0, type) value) { \
    BOOST_PP_TUPLE_ELEM(3, 1, type)[index] = value;                                             \
  }

// We currently use uint8_t as our data type for boolean values in DATA_TYPE_INFO.
// This is because using bool causes trouble in the AllParameterVariant and breaks a large number
// of tests. To be still able to access these values as booleans, we need these two helper methods,
// which we cannot generate.
template <>
bool JitVariantVector::get(const size_t index) const {
  return static_cast<bool>(Bool[index]);
}

template <>
void JitVariantVector::set(const size_t index, const bool value) {
  Bool[index] = static_cast<uint8_t>(value);
}

// Generate get and set methods for all data types defined in the DATA_TYPE_INFO
BOOST_PP_SEQ_FOR_EACH(JIT_VARIANT_VECTOR_GET, _, DATA_TYPE_INFO)
BOOST_PP_SEQ_FOR_EACH(JIT_VARIANT_VECTOR_SET, _, DATA_TYPE_INFO)

// cleanup
#undef JIT_VARIANT_VECTOR_GET
#undef JIT_VARIANT_VECTOR_SET

}  // namespace opossum
