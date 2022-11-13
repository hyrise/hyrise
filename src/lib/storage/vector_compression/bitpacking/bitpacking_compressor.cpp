#include "bitpacking_compressor.hpp"

#include <algorithm>
#include <cmath>
#include <type_traits>

#include "bitpacking_vector_type.hpp"

namespace hyrise {

class BitPackingVector;

std::unique_ptr<const BaseCompressedVector> BitPackingCompressor::compress(const pmr_vector<uint32_t>& vector,
                                                                           const PolymorphicAllocator<size_t>& alloc,
                                                                           const UncompressedVectorInfo& meta_info) {
  const auto max_element_it = std::max_element(vector.cbegin(), vector.cend());

  auto required_bits = 1u;
  if (max_element_it != vector.cend() && *max_element_it != 0) {
    // add 1 to the maximum value because log2(1) = 0 but we need one bit to represent it.
    required_bits = static_cast<uint32_t>(std::ceil(log2(*max_element_it + 1u)));
  }

  auto data = pmr_compact_vector(required_bits, vector.size(), alloc);

  /**
   * The compact_vector does not zero initialize its memory, which leads to non-reproducible tests that
   * use the binary writer (which checks written bytes one by one, writing uninitialized bytes at the end
   * of the vector thus leads to "random" bytes written to disk). The compact_vector allocates memory
   * aligned to its word size and due to this, some memory at the end is not always overwritten.
   * Hence, fill the internal memory with zeroes. Unfortunately, compact_vector does not give us a better
   * interface.
   * Therefore, we proceed to work with a raw pointer to the compact_vector's internal memory
   * and fill it with zeroes. For this, data.bytes() gives the number of allocated bytes for the internal
   * memory (word-aligned, see bitpacking_vector_type.hpp).
   * When the word size gets changed by us in the template (for example to uint32_t), this still works.
   *
   */

  using InternalType = std::remove_reference_t<decltype(*data.get())>;
  std::fill_n(data.get(), data.bytes() / sizeof(InternalType), InternalType{0});

  std::copy(vector.cbegin(), vector.cend(), data.begin());

  return std::make_unique<BitPackingVector>(std::move(data));
}

std::unique_ptr<BaseVectorCompressor> BitPackingCompressor::create_new() const {
  return std::make_unique<BitPackingCompressor>();
}

}  // namespace hyrise
