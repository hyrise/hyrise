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

#define JIT_VARIANT_VECTOR_GROW_BY_ONE(r, d, type)                                                              \
  template <>                                                                                                   \
  size_t JitVariantVector::grow_by_one<BOOST_PP_TUPLE_ELEM(3, 0, type)>(const InitialValue initial_value) {     \
    switch (initial_value) {                                                                                    \
      case InitialValue::Zero:                                                                                  \
        BOOST_PP_TUPLE_ELEM(3, 1, type).push_back(BOOST_PP_TUPLE_ELEM(3, 0, type)());                           \
        break;                                                                                                  \
      case InitialValue::MaxValue:                                                                              \
        BOOST_PP_TUPLE_ELEM(3, 1, type).push_back(std::numeric_limits<BOOST_PP_TUPLE_ELEM(3, 0, type)>::max()); \
        break;                                                                                                  \
      case InitialValue::MinValue:                                                                              \
        BOOST_PP_TUPLE_ELEM(3, 1, type).push_back(std::numeric_limits<BOOST_PP_TUPLE_ELEM(3, 0, type)>::min()); \
        break;                                                                                                  \
    }                                                                                                           \
    return BOOST_PP_TUPLE_ELEM(3, 1, type).size() - 1;                                                          \
  }

#define JIT_VARIANT_VECTOR_GET_VECTOR(r, d, type)                                                                 \
  template <>                                                                                                     \
  std::vector<BOOST_PP_TUPLE_ELEM(3, 0, type)>& JitVariantVector::get_vector<BOOST_PP_TUPLE_ELEM(3, 0, type)>() { \
    return BOOST_PP_TUPLE_ELEM(3, 1, type);                                                                       \
  }

// Generate get and set methods for all data types defined in the JIT_DATA_TYPE_INFO
BOOST_PP_SEQ_FOR_EACH(JIT_VARIANT_VECTOR_GET, _, JIT_DATA_TYPE_INFO)
BOOST_PP_SEQ_FOR_EACH(JIT_VARIANT_VECTOR_SET, _, JIT_DATA_TYPE_INFO)
BOOST_PP_SEQ_FOR_EACH(JIT_VARIANT_VECTOR_GROW_BY_ONE, _, JIT_DATA_TYPE_INFO)
BOOST_PP_SEQ_FOR_EACH(JIT_VARIANT_VECTOR_GET_VECTOR, _, JIT_DATA_TYPE_INFO)

// cleanup
#undef JIT_VARIANT_VECTOR_GET
#undef JIT_VARIANT_VECTOR_SET
#undef JIT_VARIANT_VECTOR_GROW_BY_ONE
#undef JIT_VARIANT_VECTOR_GET_VECTOR

}  // namespace opossum
