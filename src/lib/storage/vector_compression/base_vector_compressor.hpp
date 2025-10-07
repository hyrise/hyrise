#pragma once

#include <cstdint>
#include <memory>

#include "types.hpp"
#include "vector_compression.hpp"

namespace hyrise {

class BaseCompressedVector;

/**
 * @brief Base class of all vector compressors
 *
 * Sub-classes must be added in vector_compression.cpp
 */
// This class has to have a virtual destructor because it is virtual.
// NOLINTNEXTLINE(hicpp-special-member-functions,cppcoreguidelines-special-member-functions)
class BaseVectorCompressor {
 public:
  virtual ~BaseVectorCompressor() = default;

  virtual std::unique_ptr<const BaseCompressedVector> compress(const pmr_vector<uint32_t>& vector,
                                                               const PolymorphicAllocator<size_t>& alloc,
                                                               const UncompressedVectorInfo& meta_info = {}) = 0;

  virtual std::unique_ptr<BaseVectorCompressor> create_new() const = 0;
};

}  // namespace hyrise
