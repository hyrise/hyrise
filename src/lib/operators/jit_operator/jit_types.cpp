#include "jit_types.hpp"

namespace opossum {

#define JIT_VARIANT_VECTOR_GET(r, d, type)                                           \
  template <>                                                                        \
  BOOST_PP_TUPLE_ELEM(3, 0, type)                                                    \
  JitVariantVector::get<BOOST_PP_TUPLE_ELEM(3, 0, type)>(const size_t index) const { \
    return BOOST_PP_TUPLE_ELEM(3, 1, type)[index];                                   \
  }

#define JIT_VARIANT_VECTOR_SET(r, d, type)                                                                     \
  template <>                                                                                                  \
  void JitVariantVector::set<BOOST_PP_TUPLE_ELEM(3, 0, type)>(const size_t index,                              \
                                                              const BOOST_PP_TUPLE_ELEM(3, 0, type) & value) { \
    BOOST_PP_TUPLE_ELEM(3, 1, type)[index] = value;                                                            \
  }

#define JIT_VARIANT_VECTOR_RESIZE(r, d, type) BOOST_PP_TUPLE_ELEM(3, 1, type).resize(new_size);

#define JIT_VARIANT_VECTOR_GROW_BY_ONE(r, d, type)                                                                 \
  template <>                                                                                                      \
  size_t JitVariantVector::grow_by_one<BOOST_PP_TUPLE_ELEM(3, 0, type)>(const InitialValue initial_value) {        \
    _is_null.emplace_back(true);                                                                                   \
                                                                                                                   \
    switch (initial_value) {                                                                                       \
      case InitialValue::Zero:                                                                                     \
        BOOST_PP_TUPLE_ELEM(3, 1, type).emplace_back(BOOST_PP_TUPLE_ELEM(3, 0, type)());                           \
        break;                                                                                                     \
      case InitialValue::MaxValue:                                                                                 \
        BOOST_PP_TUPLE_ELEM(3, 1, type).emplace_back(std::numeric_limits<BOOST_PP_TUPLE_ELEM(3, 0, type)>::max()); \
        break;                                                                                                     \
      case InitialValue::MinValue:                                                                                 \
        BOOST_PP_TUPLE_ELEM(3, 1, type).emplace_back(std::numeric_limits<BOOST_PP_TUPLE_ELEM(3, 0, type)>::min()); \
        break;                                                                                                     \
    }                                                                                                              \
    return BOOST_PP_TUPLE_ELEM(3, 1, type).size() - 1;                                                             \
  }

#define JIT_VARIANT_VECTOR_GET_VECTOR(r, d, type)                                                                 \
  template <>                                                                                                     \
  std::vector<BOOST_PP_TUPLE_ELEM(3, 0, type)>& JitVariantVector::get_vector<BOOST_PP_TUPLE_ELEM(3, 0, type)>() { \
    return BOOST_PP_TUPLE_ELEM(3, 1, type);                                                                       \
  }

void JitVariantVector::resize(const size_t new_size) {
  BOOST_PP_SEQ_FOR_EACH(JIT_VARIANT_VECTOR_RESIZE, _, JIT_DATA_TYPE_INFO)
  _is_null.resize(new_size);
}

bool JitVariantVector::is_null(const size_t index) { return _is_null[index]; }

void JitVariantVector::set_is_null(const size_t index, const bool is_null) { _is_null[index] = is_null; }

std::vector<bool>& JitVariantVector::get_is_null_vector() { return _is_null; }

// Generate get, set, grow_by_one, and get_vector methods for all data types defined in JIT_DATA_TYPE_INFO
BOOST_PP_SEQ_FOR_EACH(JIT_VARIANT_VECTOR_GET, _, JIT_DATA_TYPE_INFO)
BOOST_PP_SEQ_FOR_EACH(JIT_VARIANT_VECTOR_SET, _, JIT_DATA_TYPE_INFO)
BOOST_PP_SEQ_FOR_EACH(JIT_VARIANT_VECTOR_GROW_BY_ONE, _, JIT_DATA_TYPE_INFO)
BOOST_PP_SEQ_FOR_EACH(JIT_VARIANT_VECTOR_GET_VECTOR, _, JIT_DATA_TYPE_INFO)

JitTupleValue::JitTupleValue(const DataType data_type, const bool is_nullable, const size_t tuple_index)
    : _data_type{data_type}, _is_nullable{is_nullable}, _tuple_index{tuple_index} {}

JitTupleValue::JitTupleValue(const std::pair<const DataType, const bool> data_type, const size_t tuple_index)
    : _data_type{data_type.first}, _is_nullable{data_type.second}, _tuple_index{tuple_index} {}

DataType JitTupleValue::data_type() const { return _data_type; }

bool JitTupleValue::is_nullable() const { return _is_nullable; }

size_t JitTupleValue::tuple_index() const { return _tuple_index; }

bool JitTupleValue::is_null(JitRuntimeContext& context) const {
  return _is_nullable && context.tuple.is_null(_tuple_index);
}

void JitTupleValue::set_is_null(const bool is_null, JitRuntimeContext& context) const {
  context.tuple.set_is_null(_tuple_index, is_null);
}

bool JitTupleValue::operator==(const JitTupleValue& other) const {
  return data_type() == other.data_type() && is_nullable() == other.is_nullable() &&
         tuple_index() == other.tuple_index();
}

JitHashmapValue::JitHashmapValue(const DataType data_type, const bool is_nullable, const size_t column_index)
    : _data_type{data_type}, _is_nullable{is_nullable}, _column_index{column_index} {}

DataType JitHashmapValue::data_type() const { return _data_type; }

bool JitHashmapValue::is_nullable() const { return _is_nullable; }

size_t JitHashmapValue::column_index() const { return _column_index; }

bool JitHashmapValue::is_null(const size_t index, JitRuntimeContext& context) const {
  return _is_nullable && context.hashmap.columns[_column_index].is_null(index);
}

void JitHashmapValue::set_is_null(const bool is_null, const size_t index, JitRuntimeContext& context) const {
  context.hashmap.columns[_column_index].set_is_null(index, is_null);
}

bool jit_expression_is_binary(const JitExpressionType expression_type) {
  switch (expression_type) {
    case JitExpressionType::Addition:
    case JitExpressionType::Subtraction:
    case JitExpressionType::Multiplication:
    case JitExpressionType::Division:
    case JitExpressionType::Modulo:
    case JitExpressionType::Power:
    case JitExpressionType::Equals:
    case JitExpressionType::NotEquals:
    case JitExpressionType::GreaterThan:
    case JitExpressionType::GreaterThanEquals:
    case JitExpressionType::LessThan:
    case JitExpressionType::LessThanEquals:
    case JitExpressionType::Like:
    case JitExpressionType::NotLike:
    case JitExpressionType::And:
    case JitExpressionType::Or:
      return true;

    case JitExpressionType::Column:
    case JitExpressionType::Between:
    case JitExpressionType::Not:
    case JitExpressionType::IsNull:
    case JitExpressionType::IsNotNull:
      return false;
  }
}

// cleanup
#undef JIT_VARIANT_VECTOR_GET
#undef JIT_VARIANT_VECTOR_SET
#undef JIT_VARIANT_VECTOR_GROW_BY_ONE
#undef JIT_VARIANT_VECTOR_GET_VECTOR

}  // namespace opossum
