#pragma once

#include "bitpacking_vector.hpp"
#include "bitpacking_vector_type.hpp"
#include "storage/vector_compression/base_vector_compressor.hpp"

namespace opossum {

class BitpackingCompressor : public BaseVectorCompressor {
 public:
  std::unique_ptr<const BaseCompressedVector> compress(const pmr_vector<uint32_t>& vector,
                                                       const PolymorphicAllocator<size_t>& alloc,
                                                       const UncompressedVectorInfo& meta_info = {}) final;

  std::unique_ptr<BaseVectorCompressor> create_new() const final;

 private:
  uint32_t _find_max_value(const pmr_vector<uint32_t>& vector) const;
  uint32_t _get_required_bits(uint32_t max_value) const;
};

}  // namespace opossum
