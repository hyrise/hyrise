#include "turboPFor_bitpacking_compressor.hpp"

#include "conf.h"
#include "bitpack.h"

namespace opossum {

class TurboPForBitpackingVector;

std::unique_ptr<const BaseCompressedVector> TurboPForBitpackingCompressor::compress(
    const pmr_vector<uint32_t>& vector, const PolymorphicAllocator<size_t>& alloc,
    const UncompressedVectorInfo& meta_info) {
  
  auto data = pmr_vector<uint8_t>(alloc);
  data.reserve(vector.size() * sizeof(uint32_t) + 1024);

  pmr_vector<uint32_t> in(vector);

  const auto max_value = meta_info.max_value ? *meta_info.max_value : _find_max_value(vector);
  const auto b = bsr32(max_value);

  uint8_t * out_end = bitpack32(in.data(), in.size(), data.data(), b);
  int bytes_written = out_end - data.data();
  data.resize(bytes_written);

  const uint8_t b_1 = static_cast<uint8_t>(b);

  return std::make_unique<TurboPForBitpackingVector>(std::move(data), vector.size(), b_1);
}

std::unique_ptr<BaseVectorCompressor> TurboPForBitpackingCompressor::create_new() const {
  return std::make_unique<TurboPForBitpackingCompressor>();
}

uint32_t TurboPForBitpackingCompressor::_find_max_value(const pmr_vector<uint32_t>& vector) const {
  uint32_t max = 0;
  for (const auto v : vector) {
    max |= v;
  }
  return max;
}


}  // namespace opossum
