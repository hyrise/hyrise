#pragma once

#include <cstdint>
#include <memory>

#include "base_encoded_vector.hpp"

namespace opossum {

class BaseAttributeEncoder {
 public:
  virtual ~BaseAttributeEncoder() = default;

  virtual void init(size_t size) = 0;
  virtual void append(uint32_t value) = 0;
  virtual void finish() = 0;
  virtual std::unique_ptr<BaseEncodedVector> get_vector() = 0;
};

}  // namespace opossum
