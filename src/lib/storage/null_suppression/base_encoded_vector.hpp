#pragma once

#include "types.hpp"

namespace opossum {

class BaseEncodedVector : private Noncopyable {
 public:
  virtual ~BaseEncodedVector() = default;

  virtual uint32_t get(const size_t i) const = 0;

  virtual size_t size() const = 0;
};
}  // namespace opossum
