#include "bitpacking_compressor.hpp"

#include <algorithm>
#include <cmath>

#include "compact_vector.hpp"

namespace opossum {

class BitPackingVector;

std::unique_ptr<const BaseCompressedVector> BitPackingCompressor::compress(const pmr_vector<uint32_t>& vector,
                                                                           const PolymorphicAllocator<size_t>& alloc,
                                                                           const UncompressedVectorInfo& meta_info) {
  const auto max_value = _find_max_value(vector);
  const auto required_bits = _get_required_bits(max_value);

  auto data = pmr_compact_vector<uint32_t>(required_bits, alloc);

  // resize to avoid allocating too much memory with auto-growing vector. Doesn't support reserve().
  data.resize(vector.size());

  // compactvector does not zero initialize it's memory, which leads to non-reproducible tests that
  // use the binary writer. Hence, fill the internal memory with zeroes.
  std::fill_n(const_cast<uint64_t*>(data.get()), data.bytes() / 8, 0);

  std::copy(vector.cbegin(), vector.cend(), data.begin());

  return std::make_unique<BitPackingVector>(std::move(data));
}

std::unique_ptr<BaseVectorCompressor> BitPackingCompressor::create_new() const {
  return std::make_unique<BitPackingCompressor>();
}

uint32_t BitPackingCompressor::_find_max_value(const pmr_vector<uint32_t>& vector) const {
  const auto it = std::max_element(vector.cbegin(), vector.cend());
  return it != vector.cend() ? *it : 0u;
}

uint32_t BitPackingCompressor::_get_required_bits(uint32_t max_value) const {
  if (max_value == 0u) {
    return 1u;
  }
  // add 1 to max_value because log2(1) = 0 but we need one bit to represent it.
  return static_cast<uint32_t>(std::ceil(log2(max_value + 1u)));
}

}  // namespace opossum
