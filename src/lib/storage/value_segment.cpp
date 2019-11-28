#include "value_segment.hpp"

#include <climits>
#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "resolve_type.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

template <typename T>
ValueSegment<T>::ValueSegment(bool nullable) : BaseValueSegment(data_type_from_type<T>()) {
  if (nullable) _null_values = pmr_concurrent_vector<bool>();
}

template <typename T>
ValueSegment<T>::ValueSegment(const PolymorphicAllocator<T>& alloc, bool nullable)
    : BaseValueSegment(data_type_from_type<T>()), _values(alloc) {
  if (nullable) _null_values = pmr_concurrent_vector<bool>(alloc);
}

template <typename T>
ValueSegment<T>::ValueSegment(pmr_concurrent_vector<T>&& values, const PolymorphicAllocator<T>& alloc)
    : BaseValueSegment(data_type_from_type<T>()), _values(std::move(values), alloc) {}

template <typename T>
ValueSegment<T>::ValueSegment(pmr_concurrent_vector<T>&& values, pmr_concurrent_vector<bool>&& null_values,
                              const PolymorphicAllocator<T>& alloc)
    : BaseValueSegment(data_type_from_type<T>()),
      _values(std::move(values), alloc),
      _null_values({std::move(null_values), alloc}) {
  DebugAssert(values.size() == null_values.size(), "The number of values and null values should be equal");
}

template <typename T>
ValueSegment<T>::ValueSegment(const std::vector<T>& values, const PolymorphicAllocator<T>& alloc)
    : BaseValueSegment(data_type_from_type<T>()), _values(values, alloc) {}

template <typename T>
ValueSegment<T>::ValueSegment(std::vector<T>&& values, const PolymorphicAllocator<T>& alloc)
    : BaseValueSegment(data_type_from_type<T>()), _values(std::move(values), alloc) {}

template <typename T>
ValueSegment<T>::ValueSegment(const std::vector<T>& values, std::vector<bool>& null_values,
                              const PolymorphicAllocator<T>& alloc)
    : BaseValueSegment(data_type_from_type<T>()),
      _values(values, alloc),
      _null_values(pmr_concurrent_vector<bool>(null_values, alloc)) {
  DebugAssert(values.size() == null_values.size(), "The number of values and null values should be equal");
}

template <typename T>
ValueSegment<T>::ValueSegment(std::vector<T>&& values, std::vector<bool>&& null_values,
                              const PolymorphicAllocator<T>& alloc)
    : BaseValueSegment(data_type_from_type<T>()),
      _values(std::move(values), alloc),
      _null_values(pmr_concurrent_vector<bool>(std::move(null_values), alloc)) {
  DebugAssert(values.size() == null_values.size(), "The number of values and null values should be equal");
}

template <typename T>
AllTypeVariant ValueSegment<T>::operator[](const ChunkOffset chunk_offset) const {
  DebugAssert(chunk_offset != INVALID_CHUNK_OFFSET, "Passed chunk offset must be valid.");
  PerformanceWarning("operator[] used");

  // Segment supports null values and value is null
  if (is_nullable() && _null_values->at(chunk_offset)) {
    return NULL_VALUE;
  }

  return _values.at(chunk_offset);
}

template <typename T>
bool ValueSegment<T>::is_null(const ChunkOffset chunk_offset) const {
  return is_nullable() && (*_null_values)[chunk_offset];
}

template <typename T>
T ValueSegment<T>::get(const ChunkOffset chunk_offset) const {
  DebugAssert(chunk_offset != INVALID_CHUNK_OFFSET, "Passed chunk offset must be valid.");

  Assert(!is_nullable() || !(*_null_values).at(chunk_offset), "Can’t return value of segment type because it is null.");
  return _values.at(chunk_offset);
}

template <typename T>
void ValueSegment<T>::append(const AllTypeVariant& val) {
  bool is_null = variant_is_null(val);

  if (is_nullable()) {
    (*_null_values).push_back(is_null);
    _values.push_back(is_null ? T{} : boost::get<T>(val));
    return;
  }

  Assert(!is_null, "ValueSegments is not nullable but value passed is null.");

  _values.push_back(boost::get<T>(val));
}

template <typename T>
void ValueSegment<T>::reserve(const size_t capacity) {
  _values.reserve(capacity);
  if (_null_values) _null_values->reserve(capacity);
}

template <typename T>
const pmr_concurrent_vector<T>& ValueSegment<T>::values() const {
  return _values;
}

template <typename T>
pmr_concurrent_vector<T>& ValueSegment<T>::values() {
  return _values;
}

template <typename T>
bool ValueSegment<T>::is_nullable() const {
  return static_cast<bool>(_null_values);
}

template <typename T>
const pmr_concurrent_vector<bool>& ValueSegment<T>::null_values() const {
  DebugAssert(is_nullable(), "This ValueSegment does not support null values.");

  return *_null_values;
}

template <typename T>
pmr_concurrent_vector<bool>& ValueSegment<T>::null_values() {
  DebugAssert(is_nullable(), "This ValueSegment does not support null values.");

  return *_null_values;
}

template <typename T>
ChunkOffset ValueSegment<T>::size() const {
  return static_cast<ChunkOffset>(_values.size());
}

template <typename T>
std::shared_ptr<BaseSegment> ValueSegment<T>::copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const {
  pmr_concurrent_vector<T> new_values(_values, alloc);  // NOLINT(cppcoreguidelines-slicing)
                                                        // (clang-tidy reports slicing that comes from tbb)
  if (is_nullable()) {
    pmr_concurrent_vector<bool> new_null_values(*_null_values, alloc);  // NOLINT(cppcoreguidelines-slicing) (see above)
    return std::allocate_shared<ValueSegment<T>>(alloc, std::move(new_values), std::move(new_null_values));
  } else {
    return std::allocate_shared<ValueSegment<T>>(alloc, std::move(new_values));
  }
}

template <typename T>
size_t ValueSegment<T>::estimate_memory_usage() const {
  size_t bool_size = 0u;
  if (_null_values) {
    bool_size = _null_values->size() * sizeof(bool);
    // Integer ceiling, since sizeof(bool) equals 1, but boolean vectors are optimized.
    bool_size = _null_values->size() % CHAR_BIT ? bool_size / CHAR_BIT + 1 : bool_size / CHAR_BIT;
  }

  return sizeof(*this) + _values.size() * sizeof(T) + bool_size;
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(ValueSegment);

}  // namespace opossum
