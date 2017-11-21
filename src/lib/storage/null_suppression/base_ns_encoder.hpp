#pragma once

#include <cstdint>
#include <memory>

#include "base_ns_vector.hpp"

namespace opossum {

class BaseNsEncoder {
 public:
  virtual ~BaseNsEncoder() = default;

  virtual void init(size_t size) = 0;
  virtual void append(uint32_t value) = 0;
  virtual void finish() = 0;
  virtual std::unique_ptr<BaseNsVector> get_vector() = 0;
};

}  // namespace opossum
