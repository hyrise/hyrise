#pragma once

#include <cstdint>

#include "types.hpp"

namespace opossum {

/**
 * @brief Base class of all vector decompressors
 *
 * Implements point-access into a compressed vector.
 *
 * Note: Make sure that implementations of these methods
 *       are marked `final` so that the compiler can omit
 *       expensive virtual method calls!
 */
class BaseVectorDecompressor {
 public:
  virtual ~BaseVectorDecompressor() = default;

  virtual uint32_t get(size_t i) = 0;
  virtual size_t size() const = 0;
};

}  // namespace opossum
