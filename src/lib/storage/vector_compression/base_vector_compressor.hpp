#pragma once

#include "vector_compression.hpp"  // NEEDEDINCLUDE

namespace opossum {

class BaseCompressedVector;

/**
 * @brief Base class of all vector compressors
 *
 * Sub-classes must be added in vector_compression.cpp
 */
class BaseVectorCompressor {
 public:
  virtual ~BaseVectorCompressor() = default;

  virtual std::unique_ptr<const BaseCompressedVector> compress(const pmr_vector<uint32_t>& vector,
                                                               const PolymorphicAllocator<size_t>& alloc,
                                                               const UncompressedVectorInfo& meta_info = {}) = 0;

  virtual std::unique_ptr<BaseVectorCompressor> create_new() const = 0;
};

}  // namespace opossum
