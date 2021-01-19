#include "turboPFOR_segment.hpp"

#include <algorithm>

#include "resolve_type.hpp"
#include "utils/performance_warning.hpp"
#include "utils/size_estimation_utils.hpp"

namespace opossum {

#define ROUND_UP(_n_, _a_) (((_n_) + ((_a_)-1)) & ~((_a_)-1))

template <typename T, typename U>
TurboPFORSegment<T, U>::TurboPFORSegment(const std::shared_ptr<pmr_vector<unsigned char>>& encoded_values,
                                       std::optional<pmr_vector<bool>> null_values,
                                       ChunkOffset size)
    : AbstractEncodedSegment(data_type_from_type<T>()),
      _encoded_values{encoded_values},
      _null_values{null_values},
      _size{size} {
        in = encoded_values->data();
        p4ini(&_p4, &in, size, &b);
      }

template <typename T, typename U>
const std::shared_ptr<pmr_vector<unsigned char>> TurboPFORSegment<T, U>::encoded_values() const {
  return _encoded_values;
}

template <typename T, typename U>
const std::optional<pmr_vector<bool>>& TurboPFORSegment<T, U>::null_values() const {
  return _null_values;
}

template <typename T, typename U>
AllTypeVariant TurboPFORSegment<T,U>::operator[](const ChunkOffset chunk_offset) const {
  PerformanceWarning("operator[] used");
  const auto typed_value = get_typed_value(chunk_offset);
  if (!typed_value) {
    return NULL_VALUE;
  }
  return *typed_value;
}

template <typename T, typename U>
ChunkOffset TurboPFORSegment<T,U>::size() const {
  return _size;
}

template <typename T, typename U>
std::shared_ptr<AbstractSegment> TurboPFORSegment<T,U>::copy_using_allocator(
    const PolymorphicAllocator<size_t>& alloc) const {
  auto new_encoded_values = std::make_shared<pmr_vector<unsigned char>>(*_encoded_values, alloc);

  std::optional<pmr_vector<bool>> new_null_values;
  if (_null_values) {
    new_null_values = pmr_vector<bool>(*_null_values, alloc);
  }
  auto copy = std::make_shared<TurboPFORSegment<T,U>>(new_encoded_values, std::move(new_null_values), _size);

  copy->access_counter = access_counter;

  return copy;
}

template <typename T, typename U>
size_t TurboPFORSegment<T,U>::memory_usage([[maybe_unused]] const MemoryUsageCalculationMode mode) const {
  size_t segment_size = sizeof(*this);
  if (_null_values) {
    segment_size += _null_values->capacity() / CHAR_BIT;
  }
  segment_size += _encoded_values->capacity() * sizeof(unsigned char);
  segment_size += 4; // size
  segment_size += 4; // b
  segment_size += 58; //vp4

  return segment_size;
}

template <typename T, typename U>
EncodingType TurboPFORSegment<T,U>::encoding_type() const {
  return EncodingType::TurboPFOR;
}

template <typename T, typename U>
std::optional<CompressedVectorType> TurboPFORSegment<T,U>::compressed_vector_type() const {
  return std::nullopt;
}

template <typename T, typename U>
uint32_t TurboPFORSegment<T,U>::get_turbopfor_value(uint32_t i) const {
  uint32_t value;
  if(unlikely(_p4.isx)) {
    value = p4getx32(const_cast<p4*>(&_p4), in, i, b);
  } else {
    value = bitgetx32(in, i, b);
  }
  return value;
}

template class TurboPFORSegment<int32_t>;
// int64_t disabled for now, todo enable
// template class FrameOfReferenceSegment<int64_t>;

}  // namespace opossum