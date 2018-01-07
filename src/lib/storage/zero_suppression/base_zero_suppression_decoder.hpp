#pragma once

#include <cstdint>

#include "types.hpp"

namespace opossum {

/**
 * @brief Base class of all zero suppression decoders
 *
 * Subclasses must be added in decoders.hpp
 */
class BaseZeroSuppressionDecoder {
 public:
  virtual ~BaseZeroSuppressionDecoder() = default;

  virtual uint32_t get(size_t i) = 0;
  virtual size_t size() const = 0;
};

}  // namespace opossum
