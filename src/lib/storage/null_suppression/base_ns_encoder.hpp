#pragma once

#include <cstdint>
#include <memory>

#include "types.hpp"

namespace opossum {

class BaseNsVector;

class BaseNsEncoder {
 public:
  virtual ~BaseNsEncoder() = default;

  virtual std::unique_ptr<BaseNsVector> encode(const pmr_vector<uint32_t>& vector,
                                               const PolymorphicAllocator<size_t>& alloc) = 0;
};

}  // namespace opossum
