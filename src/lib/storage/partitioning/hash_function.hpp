#pragma once

#include "all_type_variant.hpp"
#include "strong_typedef.hpp"
#include "abstract_hash_function.hpp"

namespace opossum {

class HashFunction : public AbstractHashFunction {
 public:
  const HashValue operator()(const AllTypeVariant& value) const override;
  HashFunctionType get_type() const override;
};

}  // namespace opossum
