#include "bitpacking_compressor.hpp"
#include "compact_vector.hpp"
#include <algorithm>

namespace opossum {

class BitpackingVector;

std::unique_ptr<const BaseCompressedVector> BitpackingCompressor::compress(
    const pmr_vector<uint32_t>& vector, const PolymorphicAllocator<size_t>& alloc,
    const UncompressedVectorInfo& meta_info) {
 
  const auto max_value = _find_max_value(vector);
  uint32_t b = std::max(compact::vector<unsigned int, 32>::required_bits(max_value), 1u);

  auto data = std::make_shared<pmr_bitpacking_vector<uint32_t>>(b, alloc);
  for (int i = 0; i < vector.size(); i++) {
    data->push_back(vector[i]);
  }

  for (int i = 0; i < vector.size(); i++) {
    uint32_t original = vector[i];
    uint32_t compressed = (*data)[i];
    if (original != compressed) {
      throw std::logic_error("Wrong!");
    }
  }

  return std::make_unique<BitpackingVector>(std::move(data));
}

std::unique_ptr<BaseVectorCompressor> BitpackingCompressor::create_new() const {
  return std::make_unique<BitpackingCompressor>();
}

uint32_t BitpackingCompressor::_find_max_value(const pmr_vector<uint32_t>& vector) const {
  uint32_t max = 0;
  for (const auto v : vector) {
    max |= v;
  }
  return max;
}

uint32_t BitpackingCompressor::_bit_width(uint32_t number) {
  int target_level = 0;
  while (number >>= 1) {
    ++target_level;
  }
  return target_level;
}


}  // namespace opossum
