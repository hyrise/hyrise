#include "bitpacking_compressor.hpp"

#include <algorithm>
#include <cmath>

#include "compact_vector.hpp"

namespace opossum {

class BitPackingVector;

std::unique_ptr<const BaseCompressedVector> BitPackingCompressor::compress(const pmr_vector<uint32_t>& vector,
                                                                           const PolymorphicAllocator<size_t>& alloc,
                                                                           const UncompressedVectorInfo& meta_info) {
  const auto max_element_it = std::max_element(vector.cbegin(), vector.cend());

  auto required_bits = 1u;
  if (max_element_it != vector.cend() && *max_element_it != 0) {
    // add 1 to max_value because log2(1) = 0 but we need one bit to represent it.
    required_bits = static_cast<uint32_t>(std::ceil(log2(*max_element_it + 1u)));
  }

  auto data = pmr_compact_vector<uint32_t>(required_bits, vector.size(), alloc);

  // compactvector does not zero initialize it's memory, which leads to non-reproducible tests that
  // use the binary writer (due to alignment, some memory at the end is not overwritten).
  // Hence, fill the internal memory with zeroes.
  std::fill_n(data.get(), data.bytes() / 8, 0);

  std::copy(vector.cbegin(), vector.cend(), data.begin());

  return std::make_unique<BitPackingVector>(std::move(data));
}

std::unique_ptr<BaseVectorCompressor> BitPackingCompressor::create_new() const {
  return std::make_unique<BitPackingCompressor>();
}

}  // namespace opossum
