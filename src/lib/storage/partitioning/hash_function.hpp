#pragma once

#include "abstract_hash_function.hpp"
#include "all_type_variant.hpp"
#include "strong_typedef.hpp"

namespace opossum {

class HashFunction : public AbstractHashFunction {
 public:
  const HashValue operator()(const AllTypeVariant& value) const override;
  HashFunctionType get_type() const override;
};

}  // namespace opossum
