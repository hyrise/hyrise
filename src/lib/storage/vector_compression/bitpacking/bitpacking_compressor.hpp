#pragma once

#include "bitpacking_vector.hpp"
#include "storage/vector_compression/base_vector_compressor.hpp"

namespace hyrise {

class BitPackingCompressor : public BaseVectorCompressor {
 public:
  std::unique_ptr<const BaseCompressedVector> compress(const pmr_vector<uint32_t>& vector,
                                                       const PolymorphicAllocator<size_t>& alloc,
                                                       const UncompressedVectorInfo& meta_info = {}) final;

  std::unique_ptr<BaseVectorCompressor> create_new() const final;
};

}  // namespace hyrise
