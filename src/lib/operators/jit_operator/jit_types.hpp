#pragma once

#include <boost/preprocessor/seq/for_each.hpp>

#include "all_type_variant.hpp"
#include "storage/base_value_column.hpp"
#include "storage/chunk.hpp"
#include "storage/column_iterables/base_column_iterators.hpp"

namespace opossum {

// We need a boolean data type in the JitOperator, but don't want to add it to
// DATA_TYPE_INFO to avoid costly template instantiations.
// See "all_type_variant.hpp" for details.
#define JIT_DATA_TYPE_INFO ((bool, Bool, "bool")) DATA_TYPE_INFO

#define JIT_VARIANT_VECTOR_MEMBER(r, d, type) \
  std::vector<BOOST_PP_TUPLE_ELEM(3, 0, type)> BOOST_PP_TUPLE_ELEM(3, 1, type);

#define JIT_VARIANT_VECTOR_RESIZE(r, d, type) BOOST_PP_TUPLE_ELEM(3, 1, type).resize(new_size);

/* A brief overview of the type system and the way values are handled in the JitOperator:
 *
 * The JitOperator performs most of its operations on variant values, since this allows writing generic operators with
 * an unlimited number of variably-typed operands (without the need for excessive templating).
 * While this sounds costly at first, the jit engine knows the actual type of each value in advance and replaces generic
 * operations with specialized versions for the concrete data types.
 *
 * This entails two important restrictions:
 * 1) There can be no temporary (=local) variant values. This is because we support std::string as a data type.
 *    Strings are not POD types and they thus require memory allocations, destructors and exception
 *    handling. The specialization engine is not able to completely optimize this overhead away, even if no strings are
 *    processed in an operation.
 * 2) The data type of each value needs to be stored separately from the value itself. This is because we want the
 *    specialization engine to know about data type, but not concrete values.
 *
 * Both restrictions are enforced using the same mechanism:
 * All values (and other data that must not be available to the specialization engine) are encapsulated in the
 * JitRuntimeContext. This context is only passed to the operator after code specialization has taken place.
 * The JitRuntimeContext contains a vector of variant values (called tuple) which the JitOperator can access through
 * JitTupleValue instances.
 * The runtime tuple is created and destroyed outside the JitOperator's scope, so no memory management is
 * required from within the operator.
 * Each JitTupleValue encapsulates information about how to access a single value in the runtime tuple. However, the
 * JitTupleValues are part of the JitOperator and must thus not store a reference to the JitRuntimeContext. So while
 * they "know" how to access values in the runtime tuple, they do not have the means to do so.
 * Only by passing the runtime context to a JitTupleValue, a JitMaterializedValue is created.
 * This materialized value contains a reference to the underlying vector and finally allows the operator to access a
 * data value.
 */

/* The JitVariantVector can be used in two ways:
 * 1) As a vector of variant values.
 *    Imagine you want to store a database row (aka tuple) of some table. Since each column of the table could have a
 *    different type, a std::vector<Variant> seems like a good choice. To store N values, the vector is resized to
 *    contain N slots and each value (representing one column) can be accessed by its index from [0, N).
 *    We do something slightly different here: Instead of one vector (where each vector element can hold any of a number
 *    of types), we create one strongly typed vector per data type. All of these vectors have size N, so each value
 *    (representing one column of the tuple) has a slot in each vector.
 *    Accessing the value at position P as an "int" will return the element at position P in the integer vector.
 *    There is no automatic type conversions happening here. Storing a value as "int" and reading it later as "double"
 *    won't work, since this accesses different memory locations in different vectors.
 *    This is not a problem, however, since the type of each value does not change throughout query execution.
 *
 * 2) As a variant vector.
 *    This data structure can also be used to store a vector of values of some unknown, but fixed type.
 *    Say you want to build an aggregate operator and need to store a column of aggregate values. All values
 *    produced will have the same type, but there is no way of knowing that type in advance.
 *    By adding a templated "push" function to the implementation below, we can add an arbitrary number of elements to
 *    the std::vector of that type. All other vectors will remain empty.
 *    This interpretation of the variant vector is not used in the code currently, but will be helpful when implementing
 *    further operators.
 */
class JitVariantVector {
 public:
  void resize(const size_t new_size) {
    BOOST_PP_SEQ_FOR_EACH(JIT_VARIANT_VECTOR_RESIZE, _, JIT_DATA_TYPE_INFO)
    _is_null.resize(new_size);
  }

  template <typename T>
  T get(const size_t index) const;
  template <typename T>
  void set(const size_t index, const T value);
  bool is_null(const size_t index) { return _is_null[index]; }
  void set_is_null(const size_t index, const bool is_null) { _is_null[index] = is_null; }

 private:
  BOOST_PP_SEQ_FOR_EACH(JIT_VARIANT_VECTOR_MEMBER, _, JIT_DATA_TYPE_INFO)
  std::vector<uint8_t> _is_null;
};

// The structure encapsulates all data available to the JitOperator at runtime,
// but NOT during code specialization.
struct JitRuntimeContext {
  uint32_t chunk_size;
  ChunkOffset chunk_offset;
  JitVariantVector tuple;
  std::vector<std::shared_ptr<JitBaseColumnIterator>> inputs;
  std::vector<std::shared_ptr<BaseValueColumn>> outputs;
  std::shared_ptr<Chunk> out_chunk;
};

// A JitMaterializedValue is a wrapper to access an actual value in the runtime context.
// While a JitTupleValue is only an abstract representation of the value (knowing how to access it, but not being able
// to actually do so), the JitMaterializedValue can access the value.
// It is usually created from a JitTupleValue by providing the context at runtime.
struct JitMaterializedValue {
  JitMaterializedValue(const DataType data_type, const bool is_nullable, const size_t vector_index,
                       JitVariantVector& vector)
      : _data_type{data_type}, _is_nullable{is_nullable}, _vector_index{vector_index}, _vector{vector} {}

  DataType data_type() const { return _data_type; }
  bool is_nullable() const { return _is_nullable; }

  template <typename T>
  const T get() const {
    return _vector.get<T>(_vector_index);
  }
  template <typename T>
  void set(const T value) {
    _vector.set<T>(_vector_index, value);
  }
  bool is_null() const { return _is_nullable && _vector.is_null(_vector_index); }
  void set_is_null(const bool is_null) const { _vector.set_is_null(_vector_index, is_null); }

 private:
  const DataType _data_type;
  const bool _is_nullable;
  const size_t _vector_index;
  JitVariantVector& _vector;
};

// The JitTupleValue represents a value in the runtime tuple.
// The JitTupleValue has information about the DataType and index of the value it represents, but it does NOT have
// a reference to the runtime tuple with the actual values.
// however, this is enough for the jit engine to optimize any operation involving the value.
// It only knows how to access the value, once it gets converted to a JitMaterializedValue by providing the runtime
// context.
class JitTupleValue {
 public:
  JitTupleValue(const DataType data_type, const bool is_nullable, const size_t tuple_index)
      : _data_type{data_type}, _is_nullable{is_nullable}, _tuple_index{tuple_index} {}
  JitTupleValue(const std::pair<const DataType, const bool> data_type, const size_t tuple_index)
      : _data_type{data_type.first}, _is_nullable{data_type.second}, _tuple_index{tuple_index} {}

  DataType data_type() const { return _data_type; }
  bool is_nullable() const { return _is_nullable; }
  size_t tuple_index() const { return _tuple_index; }

  // Converts this abstract value into an actually accessible value
  JitMaterializedValue materialize(JitRuntimeContext& ctx) const {
    return JitMaterializedValue(_data_type, _is_nullable, _tuple_index, ctx.tuple);
  }

 private:
  const DataType _data_type;
  const bool _is_nullable;
  const size_t _tuple_index;
};

// cleanup
#undef JIT_VARIANT_VECTOR_MEMBER
#undef JIT_VARIANT_VECTOR_RESIZE

}  // namespace opossum
