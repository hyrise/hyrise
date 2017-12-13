#pragma once

#include <cstdint>
#include <memory>

#include "types.hpp"

namespace opossum {

class BaseZeroSuppressionVector;

/**
 * @brief Base class of all zero suppression encoders
 *
 * Subclasses must be added in encoders.hpp
 */
class BaseZeroSuppressionEncoder {
 public:
  virtual ~BaseZeroSuppressionEncoder() = default;

  virtual std::unique_ptr<BaseZeroSuppressionVector> encode(const pmr_vector<uint32_t>& vector,
                                               const PolymorphicAllocator<size_t>& alloc) = 0;
};

}  // namespace opossum
