#include "fixed_size_byte_aligned_compressor.hpp"

namespace opossum {

std::unique_ptr<const BaseCompressedVector> FixedSizeByteAlignedCompressor::compress(
    const pmr_vector<uint32_t>& vector, const PolymorphicAllocator<size_t>& alloc,
    const UncompressedVectorInfo& meta_info) {
  const auto max_value = meta_info.max_value ? *meta_info.max_value : _find_max_value(vector);
  return _compress_using_max_value(alloc, vector, max_value);
}

std::unique_ptr<BaseVectorCompressor> FixedSizeByteAlignedCompressor::create_new() const {
  return std::make_unique<FixedSizeByteAlignedCompressor>();
}

uint32_t FixedSizeByteAlignedCompressor::_find_max_value(const pmr_vector<uint32_t>& vector) {
  const auto it = std::max_element(vector.cbegin(), vector.cend());
  return *it;
}

std::unique_ptr<BaseCompressedVector> FixedSizeByteAlignedCompressor::_compress_using_max_value(
    const PolymorphicAllocator<size_t>& alloc, const pmr_vector<uint32_t>& vector, const uint32_t max_value) {
  if (max_value <= std::numeric_limits<uint8_t>::max()) {
    return _compress_using_uint_type<uint8_t>(alloc, vector);
  } else if (max_value <= std::numeric_limits<uint16_t>::max()) {
    return _compress_using_uint_type<uint16_t>(alloc, vector);
  } else {
    return _compress_using_uint_type<uint32_t>(alloc, vector);
  }
}

template <typename UnsignedIntType>
std::unique_ptr<BaseCompressedVector> FixedSizeByteAlignedCompressor::_compress_using_uint_type(
    const PolymorphicAllocator<size_t>& alloc, const pmr_vector<uint32_t>& vector) {
  auto data = pmr_vector<UnsignedIntType>(alloc);
  data.reserve(vector.size());

  for (auto value : vector) {
    data.push_back(static_cast<UnsignedIntType>(value));
  }

  return std::make_unique<FixedSizeByteAlignedVector<UnsignedIntType>>(std::move(data));
}

}  // namespace opossum
