#pragma once

#include <cstdint>

#include "types.hpp"

namespace opossum {

/**
 * @brief Implements the virtual interface of all decoders
 */
class BaseNsDecoder {
 public:
  virtual ~BaseNsDecoder() = default;

  virtual uint32_t get(size_t i) = 0;
  virtual size_t size() const = 0;
};

}  // namespace opossum
