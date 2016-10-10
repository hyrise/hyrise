#pragma once

#include "common.hpp"
#include "types.hpp"

namespace opossum {

class BaseColumn {
 public:
  BaseColumn() = default;
  BaseColumn(BaseColumn const &) = delete;
  BaseColumn(BaseColumn &&) = default;

  virtual const AllTypeVariant operator[](const size_t i) DEV_ONLY const = 0;

  virtual void append(const AllTypeVariant &val) DEV_ONLY = 0;
  virtual size_t size() const = 0;
};
}  // namespace opossum
