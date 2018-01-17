#include "run_length_column.hpp"

#include <algorithm>

#include "storage/value_column.hpp"
#include "type_cast.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

template <typename T>
RunLengthColumn<T>::RunLengthColumn(const std::shared_ptr<const pmr_vector<T>>& values,
                                    const std::shared_ptr<const pmr_vector<ChunkOffset>>& end_positions,
                                    const T null_value)
    : _values{values}, _end_positions{end_positions}, _null_value{null_value} {}

template <typename T>
std::shared_ptr<const pmr_vector<T>> RunLengthColumn<T>::values() const {
  return _values;
}

template <typename T>
std::shared_ptr<const pmr_vector<ChunkOffset>> RunLengthColumn<T>::end_positions() const {
  return _end_positions;
}

template <typename T>
const T RunLengthColumn<T>::null_value() const {
  return _null_value;
}

template <typename T>
const AllTypeVariant RunLengthColumn<T>::operator[](const ChunkOffset chunk_offset) const {
  PerformanceWarning("operator[] used");

  const auto end_position_it = std::lower_bound(_end_positions->cbegin(), _end_positions->cend(), chunk_offset);
  const auto index = std::distance(_end_positions->cbegin(), end_position_it);

  const auto value = (*_values)[index];

  if (value == _null_value) return NULL_VALUE;

  return AllTypeVariant{value};
}

template <typename T>
size_t RunLengthColumn<T>::size() const {
  DebugAssert(_end_positions->size() > 0u, "Column size is never zero.");
  return _end_positions->back() + 1u;
}

template <typename T>
std::shared_ptr<BaseColumn> RunLengthColumn<T>::copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const {
  auto new_values = pmr_vector<T>{*_values, alloc};
  auto new_end_positions = pmr_vector<ChunkOffset>{*_end_positions, alloc};

  auto new_values_ptr = std::allocate_shared<pmr_vector<T>>(alloc, std::move(new_values));
  auto new_end_positions_ptr = std::allocate_shared<pmr_vector<ChunkOffset>>(alloc, std::move(new_end_positions));
  return std::allocate_shared<RunLengthColumn<T>>(alloc, new_values_ptr, new_end_positions_ptr, _null_value);
}

template <typename T>
EncodingType RunLengthColumn<T>::encoding_type() const {
  return EncodingType::RunLength;
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(RunLengthColumn);

}  // namespace opossum
