#pragma once

#include <algorithm>
#include <limits>

#include "storage/vector_compression/base_vector_compressor.hpp"

#include "fixed_size_byte_aligned_vector.hpp"

#include "types.hpp"

namespace opossum {

class FixedSizeByteAlignedCompressor : public BaseVectorCompressor {
 public:
  std::unique_ptr<const BaseCompressedVector> compress(const pmr_vector<uint32_t>& vector,
                                                       const PolymorphicAllocator<size_t>& alloc,
                                                       const UncompressedVectorInfo& meta_info = {}) final;

  std::unique_ptr<BaseVectorCompressor> create_new() const final;

 private:
  static uint32_t _find_max_value(const pmr_vector<uint32_t>& vector);

  static std::unique_ptr<BaseCompressedVector> _compress_using_max_value(const PolymorphicAllocator<size_t>& alloc,
                                                                         const pmr_vector<uint32_t>& vector,
                                                                         const uint32_t max_value);

  template <typename UnsignedIntType>
  static std::unique_ptr<BaseCompressedVector> _compress_using_uint_type(const PolymorphicAllocator<size_t>& alloc,
                                                                         const pmr_vector<uint32_t>& vector);
};

}  // namespace opossum
