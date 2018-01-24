#pragma once

#include <cstdint>
#include <memory>

#include "zs_vector_meta_info.hpp"

#include "types.hpp"

namespace opossum {

class BaseZeroSuppressionVector;

/**
 * @brief Base class of all zero suppression encoders
 *
 * Sub-classes must be added in encoders.hpp
 */
class BaseZeroSuppressionEncoder {
 public:
  virtual ~BaseZeroSuppressionEncoder() = default;

  virtual std::unique_ptr<BaseZeroSuppressionVector> encode(const pmr_vector<uint32_t>& vector,
                                                            const PolymorphicAllocator<size_t>& alloc,
                                                            const ZsVectorMetaInfo& meta_info = {}) = 0;

  virtual std::unique_ptr<BaseZeroSuppressionEncoder> create_new() const = 0;
};

}  // namespace opossum
