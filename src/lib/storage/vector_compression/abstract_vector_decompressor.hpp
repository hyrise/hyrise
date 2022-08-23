#pragma once

#include <cstdint>

#include "types.hpp"

namespace hyrise {

/**
 * @brief Base class of all vector decompressors
 *
 * Implements point-access into a compressed vector.
 *
 * Note: Make sure that implementations of these methods
 *       are marked `final` so that the compiler can omit
 *       expensive virtual method calls!
 */
class AbstractVectorDecompressor {
 public:
  virtual ~AbstractVectorDecompressor() = default;
  AbstractVectorDecompressor() = default;
  AbstractVectorDecompressor(const AbstractVectorDecompressor&) = default;
  AbstractVectorDecompressor(AbstractVectorDecompressor&&) = default;

  virtual uint32_t get(size_t i) = 0;
  virtual size_t size() const = 0;
};

}  // namespace hyrise
