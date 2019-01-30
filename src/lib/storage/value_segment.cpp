#include "value_segment.hpp"

#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "abstract_segment_visitor.hpp"
#include "resolve_type.hpp"
#include "type_cast.hpp"
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
const AllTypeVariant ValueSegment<T>::operator[](const ChunkOffset chunk_offset) const {
  DebugAssert(chunk_offset != INVALID_CHUNK_OFFSET, "Passed chunk offset must be valid.");
  PerformanceWarning("operator[] used");

  // Segment supports null values and value is null
  if (is_nullable() && _null_values->at(chunk_offset)) {
    return NULL_VALUE;
  }

  return _values.at(chunk_offset);
}

template <typename T>
const std::optional<T> ValueSegment<T>::get_typed_value(const ChunkOffset chunk_offset) const {
  // Column supports null values and value is null
  if (is_nullable() && (*_null_values)[chunk_offset]) {
    return std::nullopt;
  }
  return _values[chunk_offset];
}

template <typename T>
bool ValueSegment<T>::is_null(const ChunkOffset chunk_offset) const {
  return is_nullable() && (*_null_values)[chunk_offset];
}

template <typename T>
const T ValueSegment<T>::get(const ChunkOffset chunk_offset) const {
  DebugAssert(chunk_offset != INVALID_CHUNK_OFFSET, "Passed chunk offset must be valid.");

  Assert(!is_nullable() || !(*_null_values).at(chunk_offset), "Can’t return value of segment type because it is null.");
  return _values.at(chunk_offset);
}

template <typename T>
void ValueSegment<T>::append(const AllTypeVariant& val) {
  bool is_null = variant_is_null(val);

  if (is_nullable()) {
    (*_null_values).push_back(is_null);
    _values.push_back(is_null ? T{} : type_cast_variant<T>(val));
    return;
  }

  Assert(!is_null, "ValueSegments is not nullable but value passed is null.");

  _values.push_back(type_cast_variant<T>(val));
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
size_t ValueSegment<T>::size() const {
  return _values.size();
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
  return sizeof(*this) + _values.size() * sizeof(T) + (_null_values ? _null_values->size() * sizeof(bool) : 0u);
}

template <typename T>
ChunkOffset ValueSegment<T>::get_non_null_begin(const std::shared_ptr<const PosList>& position_filter) const {
  Assert(_sort_order, "The segment needs to be sorted to calculate the first bound.");

  ChunkOffset non_null_begin = 0;
  if (_sort_order.value() == OrderByMode::Ascending || _sort_order.value() == OrderByMode::Descending) {
    if (_null_values.has_value()) {
      if (position_filter) {
        non_null_begin = static_cast<ChunkOffset>(std::distance(
            position_filter->cbegin(),
            std::lower_bound(position_filter->cbegin(), position_filter->cend(), false,
                             [&](const auto& row_id, const auto& search_value) { return row_id.is_null(); })));
      } else {
        non_null_begin = static_cast<ChunkOffset>(std::distance(
            _null_values.value().cbegin(),
            std::lower_bound(_null_values.value().cbegin(), _null_values.value().cend(), false, std::greater<bool>())));
      }
    }
  }

  // std::cout << "non_null_begin " << non_null_begin << std::endl;

  return non_null_begin;
}

template <typename T>
ChunkOffset ValueSegment<T>::get_non_null_end(const std::shared_ptr<const PosList>& position_filter) const {
  Assert(_sort_order, "The segment needs to be sorted to calculate the first bound.");

  auto non_null_end = static_cast<ChunkOffset>(position_filter ? position_filter->size() : _values.size());
  if (_sort_order.value() == OrderByMode::AscendingNullsLast ||
      _sort_order.value() == OrderByMode::DescendingNullsLast) {
    if (_null_values.has_value()) {
      if (position_filter) {
        non_null_end = static_cast<ChunkOffset>(std::distance(
            position_filter->cbegin(),
            std::lower_bound(position_filter->cbegin(), position_filter->cend(), true,
                             [&](const auto& row_id, const auto& search_value) { return !row_id.is_null(); })));
      } else {
        non_null_end = static_cast<ChunkOffset>(
            std::distance(_null_values.value().cbegin(),
                          std::lower_bound(_null_values.value().cbegin(), _null_values.value().cend(), true)));
      }
    }
  }

  // std::cout << "non_null_end " << non_null_end << std::endl;

  return non_null_end;
}

template <typename T>
ChunkOffset ValueSegment<T>::get_first_bound(const AllTypeVariant& search_value,
                                             const std::shared_ptr<const PosList>& position_filter) const {
  Assert(_sort_order, "The segment needs to be sorted to calculate the first bound.");

  const auto non_null_begin = get_non_null_begin(position_filter);
  const auto non_null_end = get_non_null_end(position_filter);

  const auto casted_search_value = type_cast_variant<T>(search_value);

  // TODO(cmfcmf): Quite a bit of duplication.
  if (_sort_order.value() == OrderByMode::Ascending || _sort_order.value() == OrderByMode::AscendingNullsLast) {
    if (position_filter != nullptr) {
      const auto result = std::lower_bound(
          position_filter->cbegin(), position_filter->cend(), casted_search_value,
          [&](const auto& row_id, const auto& search) { return _values[row_id.chunk_offset] < search; });
      if (result == position_filter->cend()) {
        return INVALID_CHUNK_OFFSET;
      }
      return static_cast<ChunkOffset>(std::distance(position_filter->cbegin(), result));
    } else {
      const auto begin = _values.cbegin() + non_null_begin;
      const auto end = _values.cbegin() + non_null_end;
      const auto result = std::lower_bound(begin, end, casted_search_value);
      if (result == end) {
        return INVALID_CHUNK_OFFSET;
      }
      return static_cast<ChunkOffset>(std::distance(_values.cbegin(), result));
    }
  } else {
    if (position_filter != nullptr) {
      const auto result = std::lower_bound(
          position_filter->cbegin(), position_filter->cend(), casted_search_value,
          [&](const auto& row_id, const auto& search) { return _values[row_id.chunk_offset] > search; });
      if (result == position_filter->cend()) {
        return INVALID_CHUNK_OFFSET;
      }
      return static_cast<ChunkOffset>(std::distance(position_filter->cbegin(), result));
    } else {
      const auto begin = _values.cbegin() + non_null_begin;
      const auto end = _values.cbegin() + non_null_end;
      const auto result = std::lower_bound(begin, end, casted_search_value, std::greater<T>());
      if (result == end) {
        return INVALID_CHUNK_OFFSET;
      }
      return static_cast<ChunkOffset>(std::distance(_values.cbegin(), result));
    }
  }
}

template <typename T>
ChunkOffset ValueSegment<T>::get_last_bound(const AllTypeVariant& search_value,
                                            const std::shared_ptr<const PosList>& position_filter) const {
  Assert(_sort_order, "The segment needs to be sorted to calculate the last bound.");

  const auto non_null_begin = get_non_null_begin(position_filter);
  const auto non_null_end = get_non_null_end(position_filter);

  const auto casted_search_value = type_cast_variant<T>(search_value);

  // TODO(cmfcmf): Quite a bit of duplication.
  if (_sort_order.value() == OrderByMode::Ascending || _sort_order.value() == OrderByMode::AscendingNullsLast) {
    if (position_filter != nullptr) {
      const auto result = std::upper_bound(
          position_filter->cbegin(), position_filter->cend(), casted_search_value,
          [&](const auto& search, const auto& row_id) { return search < _values[row_id.chunk_offset]; });
      if (result == position_filter->cend()) {
        return INVALID_CHUNK_OFFSET;
      }
      return static_cast<ChunkOffset>(std::distance(position_filter->cbegin(), result));
    } else {
      const auto begin = _values.cbegin() + non_null_begin;
      const auto end = _values.cbegin() + non_null_end;
      const auto result = std::upper_bound(begin, end, casted_search_value);
      if (result == end) {
        return INVALID_CHUNK_OFFSET;
      }
      return static_cast<ChunkOffset>(std::distance(_values.cbegin(), result));
    }
  } else {
    if (position_filter != nullptr) {
      const auto result = std::upper_bound(
          position_filter->cbegin(), position_filter->cend(), casted_search_value,
          [&](const auto& search, const auto& row_id) { return search > _values[row_id.chunk_offset]; });
      if (result == position_filter->cend()) {
        return INVALID_CHUNK_OFFSET;
      }
      return static_cast<ChunkOffset>(std::distance(position_filter->cbegin(), result));
    } else {
      const auto begin = _values.cbegin() + non_null_begin;
      const auto end = _values.cbegin() + non_null_end;
      const auto result = std::upper_bound(begin, end, casted_search_value, std::greater<T>());
      if (result == end) {
        return INVALID_CHUNK_OFFSET;
      }
      return static_cast<ChunkOffset>(std::distance(_values.cbegin(), result));
    }
  }
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(ValueSegment);

}  // namespace opossum
