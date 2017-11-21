#pragma once

#include "ns_type.hpp"

#include "types.hpp"

namespace opossum {

class BaseNsVector : private Noncopyable {
 public:
  BaseNsVector(NsType type) : _type{type} {}

  virtual ~BaseNsVector() = default;

  virtual size_t size() const = 0;
  virtual size_t data_size() const = 0;

  NsType type() const { return _type; }

 private:
  const NsType _type;
};

}  // namespace opossum
