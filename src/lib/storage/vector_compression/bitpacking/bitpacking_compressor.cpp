#include "bitpacking_compressor.hpp"

#include <algorithm>
#include <cmath>

#include "compact_vector.hpp"

namespace opossum {

class BitpackingVector;

std::unique_ptr<const BaseCompressedVector> BitpackingCompressor::compress(const pmr_vector<uint32_t>& vector,
                                                                           const PolymorphicAllocator<size_t>& alloc,
                                                                           const UncompressedVectorInfo& meta_info) {
  const auto max_value = _find_max_value(vector);
  const auto required_bits = _get_required_bits(max_value);

  auto data = pmr_bitpacking_vector<uint32_t>(required_bits, alloc);
  // resize to avoid allocating too much memory with auto-growing vector. Doesn't support reserve().
  data.resize(vector.size());  
  std::copy(vector.cbegin(), vector.cend(), data.begin());

  return std::make_unique<BitpackingVector>(std::move(data));
}

std::unique_ptr<BaseVectorCompressor> BitpackingCompressor::create_new() const {
  return std::make_unique<BitpackingCompressor>();
}

uint32_t BitpackingCompressor::_find_max_value(const pmr_vector<uint32_t>& vector) const {
  const auto it = std::max_element(vector.cbegin(), vector.cend());
  return it != vector.cend() ? *it : 0;
}

uint32_t BitpackingCompressor::_get_required_bits(uint32_t max_value) const {
  if (max_value == 0) {
    return 1;
  }
  return static_cast<uint32_t>(std::ceil(log2(max_value + 1)));
}

}  // namespace opossum
