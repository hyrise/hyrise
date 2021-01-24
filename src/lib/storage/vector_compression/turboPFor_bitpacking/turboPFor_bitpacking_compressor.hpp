#pragma once
#define TURBOPFOR_DAC

#include "storage/vector_compression/base_vector_compressor.hpp"

#include "turboPFor_bitpacking_vector.hpp"

#include "types.hpp"

namespace opossum {

class TurboPForBitpackingCompressor : public BaseVectorCompressor {
 public:
  std::unique_ptr<const BaseCompressedVector> compress(const pmr_vector<uint32_t>& vector,
                                                       const PolymorphicAllocator<size_t>& alloc,
                                                       const UncompressedVectorInfo& meta_info = {}) final;

  std::unique_ptr<BaseVectorCompressor> create_new() const final;
    
  private:
    uint32_t _find_max_value(const pmr_vector<uint32_t>& vector) const;
};

}  // namespace opossum
