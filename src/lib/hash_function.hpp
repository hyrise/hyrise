#pragma once

#include "all_type_variant.hpp"
#include "strong_typedef.hpp"

namespace opossum {

class HashFunction {
 public:
  const HashValue calculate_hash(const AllTypeVariant value_to_hash) const;
};

}  // namespace opossum
