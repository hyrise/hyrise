#pragma once

#include "bitpacking_vector.hpp"
#include "storage/vector_compression/abstract_vector_compressor.hpp"

namespace hyrise {

class BitPackingCompressor : public AbstractVectorCompressor {
 public:
  std::unique_ptr<const AbstractCompressedVector> compress(const pmr_vector<uint32_t>& vector,
                                                       const PolymorphicAllocator<size_t>& alloc,
                                                       const UncompressedVectorInfo& meta_info = {}) final;

  std::unique_ptr<AbstractVectorCompressor> create_new() const final;
};

}  // namespace hyrise
