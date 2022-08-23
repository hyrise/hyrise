#pragma once

#include <cstdint>
#include <memory>

#include "vector_compression.hpp"

#include "types.hpp"

namespace hyrise {

class AbstractCompressedVector;

/**
 * @brief Base class of all vector compressors
 *
 * Sub-classes must be added in vector_compression.cpp
 */
class AbstractVectorCompressor {
 public:
  virtual ~AbstractVectorCompressor() = default;

  virtual std::unique_ptr<const AbstractCompressedVector> compress(const pmr_vector<uint32_t>& vector,
                                                               const PolymorphicAllocator<size_t>& alloc,
                                                               const UncompressedVectorInfo& meta_info = {}) = 0;

  virtual std::unique_ptr<AbstractVectorCompressor> create_new() const = 0;
};

}  // namespace hyrise
