#pragma once

#include "all_type_variant.hpp"
#include "storage/base_value_column.hpp"
#include "storage/chunk.hpp"
#include "storage/column_iterables/base_column_iterators.hpp"

namespace opossum {

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
 * This materialized value finally allows the operator to access a data value.
 */

/* Since we handle arithmetic and logical operations in the same way inside the JitOperator,
 * we need an additional boolean type. Booleans are stored as uint8_t, since std::vector<bool>
 * is implemented in space-efficient manner in most standard library implementations and does not support
 * all necessary operations (e.g. accessing elements as non-const references).
 * Null, on the other hand, is not a valid data type. Instead, each value must have a (potential) concrete data type.
 * Nullable values have an additional boolean value indicating their NULL status.
 */
#define JIT_DATA_TYPE_INFO ((uint8_t, Bool, "bool")) DATA_TYPE_INFO

enum class JitDataType : uint8_t { Bool, BOOST_PP_SEQ_ENUM(DATA_TYPE_ENUM_VALUES) };

// This templated method returns the JitDataType for a given type.
template <typename T>
JitDataType jit_data_type();

// A vector of variant values.
// In order to store N variant values, one vector of size N is created per data type.
// Each of the N variant values is then represented by the elements at some index I in each vector.
class JitVariantVector {
 public:
  void resize(const size_t new_size) {
    _bool.resize(new_size);
    _int.resize(new_size);
    _long.resize(new_size);
    _float.resize(new_size);
    _double.resize(new_size);
    _string.resize(new_size);
    _is_null.resize(new_size);
  }

  template <typename T>
  T& as(const size_t index);
  uint8_t& is_null(const size_t index) { return _is_null[index]; }

 private:
  std::vector<uint8_t> _bool;
  std::vector<int32_t> _int;
  std::vector<int64_t> _long;
  std::vector<float> _float;
  std::vector<double> _double;
  std::vector<std::string> _string;
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

struct JitMaterializedValue {
  JitMaterializedValue(const JitDataType data_type, const bool is_nullable, const size_t vector_index,
                       JitVariantVector& vector)
      : _data_type{data_type}, _is_nullable{is_nullable}, _vector_index{vector_index}, _vector{vector} {}

  JitDataType data_type() const { return _data_type; }
  bool is_nullable() const { return _is_nullable; }

  template <typename T>
  T& as() {
    return _vector.as<T>(_vector_index);
  }
  template <typename T>
  const T& as() const {
    return _vector.as<T>(_vector_index);
  }
  uint8_t& is_null() { return _vector.is_null(_vector_index); }
  uint8_t is_null() const { return _is_nullable && _vector.is_null(_vector_index); }

 private:
  const JitDataType _data_type;
  const bool _is_nullable;
  const size_t _vector_index;
  JitVariantVector& _vector;
};

class JitTupleValue {
 public:
  JitTupleValue(const JitDataType data_type, const bool is_nullable, const size_t tuple_index)
      : _data_type{data_type}, _is_nullable{is_nullable}, _tuple_index{tuple_index} {}
  JitTupleValue(const std::pair<const JitDataType, const bool> data_type, const size_t tuple_index)
      : _data_type{data_type.first}, _is_nullable{data_type.second}, _tuple_index{tuple_index} {}

  JitDataType data_type() const { return _data_type; }
  bool is_nullable() const { return _is_nullable; }
  size_t tuple_index() const { return _tuple_index; }

  // Converts this abstract value into an actually accessible value
  JitMaterializedValue materialize(JitRuntimeContext& ctx) const {
    return JitMaterializedValue(_data_type, _is_nullable, _tuple_index, ctx.tuple);
  }

 private:
  const JitDataType _data_type;
  const bool _is_nullable;
  const size_t _tuple_index;
};

}  // namespace opossum
