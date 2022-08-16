#include "fixed_width_integer_compressor.hpp"

namespace hyrise {

std::unique_ptr<const BaseCompressedVector> FixedWidthIntegerCompressor::compress(
    const pmr_vector<uint32_t>& vector, const PolymorphicAllocator<size_t>& alloc,
    const UncompressedVectorInfo& meta_info) {
  const auto max_value = meta_info.max_value ? *meta_info.max_value : _find_max_value(vector);
  return _compress_using_max_value(alloc, vector, max_value);
}

std::unique_ptr<BaseVectorCompressor> FixedWidthIntegerCompressor::create_new() const {
  return std::make_unique<FixedWidthIntegerCompressor>();
}

uint32_t FixedWidthIntegerCompressor::_find_max_value(const pmr_vector<uint32_t>& vector) {
  const auto iter = std::max_element(vector.cbegin(), vector.cend());
  return *iter;
}

std::unique_ptr<BaseCompressedVector> FixedWidthIntegerCompressor::_compress_using_max_value(
    const PolymorphicAllocator<size_t>& alloc, const pmr_vector<uint32_t>& vector, const uint32_t max_value) {
  if (max_value <= std::numeric_limits<uint8_t>::max()) {
    return _compress_using_uint_type<uint8_t>(alloc, vector);
  }

  if (max_value <= std::numeric_limits<uint16_t>::max()) {
    return _compress_using_uint_type<uint16_t>(alloc, vector);
  }

  return _compress_using_uint_type<uint32_t>(alloc, vector);
}

template <typename UnsignedIntType>
std::unique_ptr<BaseCompressedVector> FixedWidthIntegerCompressor::_compress_using_uint_type(
    const PolymorphicAllocator<size_t>& alloc, const pmr_vector<uint32_t>& vector) {
  auto data = pmr_vector<UnsignedIntType>(alloc);
  data.reserve(vector.size());

  for (auto value : vector) {
    data.push_back(static_cast<UnsignedIntType>(value));
  }

  return std::make_unique<FixedWidthIntegerVector<UnsignedIntType>>(std::move(data));
}

}  // namespace hyrise
