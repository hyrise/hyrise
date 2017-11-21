#pragma once

#include "types.hpp"

namespace opossum {

class BaseNsVector : private Noncopyable {
 public:
  virtual ~BaseNsVector() = default;

  virtual uint32_t get(const size_t i) const = 0;

  virtual size_t size() const = 0;
};
}  // namespace opossum
