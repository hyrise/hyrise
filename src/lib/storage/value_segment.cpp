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
#include "utils/size_estimation_utils.hpp"

namespace opossum {

template <typename T>
ValueSegment<T>::ValueSegment(bool nullable, ChunkOffset capacity) : BaseValueSegment(data_type_from_type<T>()) {
  _values.reserve(capacity);
  if (nullable) {
    _null_values = pmr_vector<bool>();
    _null_values->reserve(capacity);
  }
}

template <typename T>
ValueSegment<T>::ValueSegment(pmr_vector<T>&& values)
    : BaseValueSegment(data_type_from_type<T>()), _values(std::move(values)) {}

template <typename T>
ValueSegment<T>::ValueSegment(pmr_vector<T>&& values, pmr_vector<bool>&& null_values)
    : BaseValueSegment(data_type_from_type<T>()), _values(std::move(values)), _null_values(std::move(null_values)) {
  DebugAssert(_values.size() == _null_values->size(), "The number of values and null_values should be equal");

  // We cannot check for the capacity being equal because of the implementation details of vector<bool>
  DebugAssert(_values.capacity() <= _null_values->capacity(),
              "The capacity of values and null_values should be compatible");
}

template <typename T>
AllTypeVariant ValueSegment<T>::operator[](const ChunkOffset chunk_offset) const {
  DebugAssert(chunk_offset != INVALID_CHUNK_OFFSET, "Passed chunk offset must be valid.");
  PerformanceWarning("operator[] used");
  access_counter[SegmentAccessCounter::AccessType::Point] += 1;

  // Segment supports null values and value is null
  if (is_nullable() && _null_values->at(chunk_offset)) {
    return NULL_VALUE;
  }

  return _values.at(chunk_offset);
}

template <typename T>
bool ValueSegment<T>::is_null(const ChunkOffset chunk_offset) const {
  access_counter[SegmentAccessCounter::AccessType::Point] += 1;
  return is_nullable() && (*_null_values)[chunk_offset];
}

template <typename T>
T ValueSegment<T>::get(const ChunkOffset chunk_offset) const {
  DebugAssert(chunk_offset != INVALID_CHUNK_OFFSET, "Passed chunk offset must be valid.");

  Assert(!is_nullable() || !(*_null_values).at(chunk_offset), "Can’t return value of segment type because it is null.");
  access_counter[SegmentAccessCounter::AccessType::Point] += 1;
  return _values.at(chunk_offset);
}

template <typename T>
void ValueSegment<T>::append(const AllTypeVariant& val) {
  Assert(size() < _values.capacity(), "ValueSegment is full");

  bool is_null = variant_is_null(val);
  access_counter[SegmentAccessCounter::AccessType::Point] += 1;

  if (is_nullable()) {
    (*_null_values).push_back(is_null);
    _values.push_back(is_null ? T{} : boost::get<T>(val));
    return;
  }

  Assert(!is_null, "ValueSegment is not nullable but value passed is null.");

  _values.push_back(boost::get<T>(val));
}

template <typename T>
const pmr_vector<T>& ValueSegment<T>::values() const {
  return _values;
}

template <typename T>
pmr_vector<T>& ValueSegment<T>::values() {
  return _values;
}

template <typename T>
bool ValueSegment<T>::is_nullable() const {
  return static_cast<bool>(_null_values);
}

template <typename T>
const pmr_vector<bool>& ValueSegment<T>::null_values() const {
  DebugAssert(is_nullable(), "This ValueSegment does not support null values.");

  return *_null_values;
}

template <typename T>
void ValueSegment<T>::set_null_value(const ChunkOffset chunk_offset) {
  Assert(is_nullable(), "This ValueSegment does not support null values.");

  std::lock_guard<std::mutex> lock{_null_value_modification_mutex};
  (*_null_values)[chunk_offset] = true;
}

template <typename T>
ChunkOffset ValueSegment<T>::size() const {
  return static_cast<ChunkOffset>(_values.size());
}

template <typename T>
void ValueSegment<T>::resize(const size_t size) {
  DebugAssert(size > _values.size() && size <= _values.capacity(),
              "ValueSegments should not be shrunk or resized beyond their original capacity");
  _values.resize(size);
  if (is_nullable()) {
    std::lock_guard<std::mutex> lock{_null_value_modification_mutex};
    _null_values->resize(size);
  }
}

template <typename T>
std::shared_ptr<BaseSegment> ValueSegment<T>::copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const {
  pmr_vector<T> new_values(_values, alloc);  // NOLINT(cppcoreguidelines-slicing)
  std::shared_ptr<BaseSegment> copy;
  if (is_nullable()) {
    pmr_vector<bool> new_null_values(*_null_values, alloc);  // NOLINT(cppcoreguidelines-slicing) (see above)
    copy = std::make_shared<ValueSegment<T>>(std::move(new_values), std::move(new_null_values));
  } else {
    copy = std::make_shared<ValueSegment<T>>(std::move(new_values));
  }
  copy->access_counter = access_counter;
  return copy;
}

template <typename T>
size_t ValueSegment<T>::memory_usage([[maybe_unused]] const MemoryUsageCalculationMode mode) const {
  auto null_value_vector_size = size_t{0};
  if (_null_values) {
    null_value_vector_size = _null_values->capacity() / CHAR_BIT;
  }

  const auto common_elements_size = sizeof(*this) + null_value_vector_size;

  if constexpr (std::is_same_v<T, pmr_string>) {  // NOLINT
    return common_elements_size + string_vector_memory_usage(_values, mode);
  }

  return common_elements_size + _values.capacity() * sizeof(T);
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(ValueSegment);

}  // namespace opossum
