#include "bitpacking_compressor.hpp"

#include <algorithm>
#include <cmath>

#include "bitpacking_vector_type.hpp"

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

  auto data = pmr_compact_vector(required_bits, vector.size(), alloc);

  /**
   * The compact::vector does not zero initialize its memory, which leads to non-reproducible tests that
   * use the binary writer. The compact::vector allocates memory aligned to its word size and due to this,
   * some memory at the end is not always overwritten. Hence, fill the internal memory with zeroes.
   * Unfortunately, compact::vector does not give us a better interface. Therefore, we proceed to work with
   * a raw pointer to the compact::vector's internal memory and fill it with zeroes. For this, data.bytes() 
   * gives the number of allocated bytes for the internal memory 
   * (word-aligned, see bitpacking_vector_type.hpp). 
   *
   */
  std::fill_n(data.get(), data.bytes() / sizeof(*data.get()), 0ULL);

  std::copy(vector.cbegin(), vector.cend(), data.begin());

  return std::make_unique<BitPackingVector>(std::move(data));
}

std::unique_ptr<BaseVectorCompressor> BitPackingCompressor::create_new() const {
  return std::make_unique<BitPackingCompressor>();
}

}  // namespace opossum
