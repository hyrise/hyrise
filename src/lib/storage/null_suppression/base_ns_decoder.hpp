#pragma once

#include <cstdint>

#include "types.hpp"

namespace opossum {

/**
 * @brief Base class of all null suppression decoders
 *
 * Subclasses must be added in ns_decoders.hpp
 */
class BaseNsDecoder {
 public:
  virtual ~BaseNsDecoder() = default;

  virtual uint32_t get(size_t i) = 0;
  virtual size_t size() const = 0;
};

}  // namespace opossum
