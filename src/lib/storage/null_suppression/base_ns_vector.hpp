#pragma once

#include "ns_type.hpp"

#include "types.hpp"

namespace opossum {

class BaseNsVector : private Noncopyable {
 public:
  virtual ~BaseNsVector() = default;

  virtual size_t size() const = 0;
  virtual size_t data_size() const = 0;

  virtual NsType type() const = 0;
};

}  // namespace opossum
