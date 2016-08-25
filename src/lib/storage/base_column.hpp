#pragma once

#include "common.hpp"
#include "types.hpp"

namespace opossum {

class base_column {
 public:
  base_column() {}
  base_column(base_column const &) = delete;
  base_column(base_column &&) = default;

  virtual all_type_variant operator[](const size_t i) DEV_ONLY const = 0;

  virtual void append(const all_type_variant &val) DEV_ONLY = 0;
  virtual size_t size() const = 0;
};
}  // namespace opossum
