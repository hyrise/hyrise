#pragma once

#include "all_type_variant.hpp"
#include "strong_typedef.hpp"

namespace opossum {

class HashFunction {
 public:
  virtual const HashValue operator()(const AllTypeVariant& value) const;
};

}  // namespace opossum
