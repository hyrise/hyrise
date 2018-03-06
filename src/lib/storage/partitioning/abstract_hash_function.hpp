#pragma once

#include "all_type_variant.hpp"
#include "strong_typedef.hpp"

namespace opossum {

class AbstractHashFunction {
 public:
  virtual ~AbstractHashFunction() {};
  
  virtual const HashValue operator()(const AllTypeVariant& value) const = 0;
  virtual HashFunctionType get_type() const = 0;
};

}  // namespace opossum
