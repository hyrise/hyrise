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

// Generate get and set methods for all data types defined in the JIT_DATA_TYPE_INFO
BOOST_PP_SEQ_FOR_EACH(JIT_VARIANT_VECTOR_GET, _, JIT_DATA_TYPE_INFO)
BOOST_PP_SEQ_FOR_EACH(JIT_VARIANT_VECTOR_SET, _, JIT_DATA_TYPE_INFO)

// cleanup
#undef JIT_VARIANT_VECTOR_GET
#undef JIT_VARIANT_VECTOR_SET

}  // namespace opossum
