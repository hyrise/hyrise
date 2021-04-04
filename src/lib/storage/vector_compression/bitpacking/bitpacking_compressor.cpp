#include "bitpacking_compressor.hpp"
#include "compact_vector.hpp"
#include <algorithm>
#include "math.h"

namespace opossum {

class BitpackingVector;

std::unique_ptr<const BaseCompressedVector> BitpackingCompressor::compress(
    const pmr_vector<uint32_t>& vector, const PolymorphicAllocator<size_t>& alloc,
    const UncompressedVectorInfo& meta_info) {

  const auto max_value = _find_max_value(vector);
  const auto max_value_1 = _find_max_value_1(vector);
  const auto max_value_2 = meta_info.max_value ? *meta_info.max_value : max_value;
  
  if (max_value != max_value_1 || max_value_1 != max_value_2) {
    std::cerr << "NOT EQUAL" << max_value << "   " << max_value_1 << "  " << max_value_2 << std::endl;
  }
  
  auto b = _get_b(max_value);
  auto b_1 = _get_b(max_value_1);
  auto b_2 = _get_b(max_value_2);

  if (b != b_1 || b_1 != b_2) {
    std::cerr << "B NOT EQUAL" << b << "   " << b_1 << "  " << b_2 << std::endl;
  }

  auto data = pmr_bitpacking_vector<uint32_t>(b, alloc);
  data.resize(vector.size());
  std::copy(vector.begin(), vector.end(), data.begin());

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

uint32_t BitpackingCompressor::_find_max_value_1(const pmr_vector<uint32_t>& vector) const {
  const auto it = std::max_element(vector.cbegin(), vector.cend());
  return *it;
}

uint32_t BitpackingCompressor::_get_b(uint32_t max_value) const {
  uint32_t b_1;
  if (max_value <= 0) {
    b_1 = 1;
  } else if (max_value == 1) {
    b_1 = 1;
  } else {
    b_1 = static_cast<uint32_t>(std::ceil(log2(max_value + 1)));
  }
  uint32_t b = std::min(b_1, std::max(compact::vector<unsigned int, 32>::required_bits(max_value), 1u));
  return b;
}


}  // namespace opossum
