#include "simdCAI_segment.hpp"

#include <algorithm>

#include "resolve_type.hpp"
#include "utils/performance_warning.hpp"
#include "utils/size_estimation_utils.hpp"

namespace opossum {

template <typename T, typename U>
SIMDCAISegment<T, U>::SIMDCAISegment(const std::shared_ptr<const pmr_vector<uint32_t>>& encoded_values,
                                       std::optional<const pmr_vector<bool>> null_values,
                                       const uint8_t codec_id)
    : AbstractEncodedSegment(data_type_from_type<T>()),
      _encoded_values{encoded_values},
      _null_values{null_values},
      _codec_id{codec_id} {}

template <typename T, typename U>
const std::shared_ptr<const pmr_vector<uint32_t>> SIMDCAISegment<T, U>::encoded_values() const {
  return _encoded_values;
}

template <typename T, typename U>
const std::optional<const pmr_vector<bool>>& SIMDCAISegment<T, U>::null_values() const {
  return _null_values;
}

template <typename T, typename U>
uint8_t SIMDCAISegment<T, U>::codec_id() const {
  return _codec_id;
}

template <typename T, typename U>
AllTypeVariant SIMDCAISegment<T,U>::operator[](const ChunkOffset chunk_offset) const {
  PerformanceWarning("operator[] used");
  const auto typed_value = get_typed_value(chunk_offset);
  if (!typed_value) {
    return NULL_VALUE;
  }
  return *typed_value;
}

template <typename T, typename U>
ChunkOffset SIMDCAISegment<T,U>::size() const {
  return _null_values->size();
}

template <typename T, typename U>
std::shared_ptr<AbstractSegment> SIMDCAISegment<T,U>::copy_using_allocator(
    const PolymorphicAllocator<size_t>& alloc) const {
  auto new_encoded_values = std::make_shared<pmr_vector<uint32_t>>(*_encoded_values, alloc);

  std::optional<pmr_vector<bool>> new_null_values;
  if (_null_values) {
    new_null_values = pmr_vector<bool>(*_null_values, alloc);
  }
  auto copy = std::make_shared<SIMDCAISegment<T,U>>(new_encoded_values, std::move(new_null_values), _codec_id);

  copy->access_counter = access_counter;

  return copy;
}

template <typename T, typename U>
size_t SIMDCAISegment<T,U>::memory_usage([[maybe_unused]] const MemoryUsageCalculationMode mode) const {
  size_t segment_size = sizeof(*this);
  if (_null_values) {
    segment_size += _null_values->capacity() / CHAR_BIT;
  }
  segment_size += _encoded_values->capacity() * sizeof(uint32_t) + 1;
  return segment_size;
}

template <typename T, typename U>
EncodingType SIMDCAISegment<T,U>::encoding_type() const {
  return EncodingType::SIMDCAI;
}

template <typename T, typename U>
std::optional<CompressedVectorType> SIMDCAISegment<T,U>::compressed_vector_type() const {
  return std::nullopt;
}

template class SIMDCAISegment<int32_t>;
// int64_t disabled for now, todo enable
// template class FrameOfReferenceSegment<int64_t>;

}  // namespace opossum