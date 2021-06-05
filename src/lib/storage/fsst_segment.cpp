#include "fsst_segment.hpp"

#include <climits>
#include <sstream>
#include <string>

#include "resolve_type.hpp"
#include "storage/vector_compression/base_compressed_vector.hpp"
#include "storage/vector_compression/base_vector_decompressor.hpp"
#include "storage/vector_compression/resolve_compressed_vector_type.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

template <typename T>
FSSTSegment<T>::FSSTSegment() : AbstractEncodedSegment{data_type_from_type<T>()} {}

template <typename T>
AllTypeVariant FSSTSegment<T>::operator[](const ChunkOffset chunk_offset) const {
  PerformanceWarning("operator[] used");
  DebugAssert(chunk_offset < size(), "Passed chunk offset must be valid.");

  // TODO add real values
  return T{};

}

template <typename T>
std::optional<T> FSSTSegment<T>::get_typed_value(const ChunkOffset chunk_offset) const {
  // TODO add real values
  return std::nullopt;
}

template <typename T>
ChunkOffset FSSTSegment<T>::size() const {
  // TODO add real values
  return static_cast<ChunkOffset>(0);
}

template <typename T>
std::shared_ptr<AbstractSegment> FSSTSegment<T>::copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const {
  // TODO add real values
  return std::shared_ptr<FSSTSegment<T>>();
}

template <typename T>
size_t FSSTSegment<T>::memory_usage(const MemoryUsageCalculationMode) const {
  // TODO add real values
  return size_t{0};
}

template <typename T>
EncodingType FSSTSegment<T>::encoding_type() const {
  // TODO add real values
  return EncodingType::FSST;
}

template <typename T>
std::optional<CompressedVectorType> FSSTSegment<T>::compressed_vector_type() const {
  // TODO add real values
  return std::nullopt;
}

template <>
std::optional<CompressedVectorType> FSSTSegment<pmr_string>::compressed_vector_type() const {
  // TODO add real values
  return std::nullopt;
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(FSSTSegment);

}  // namespace opossum
