#pragma once

#include "types.hpp"

class BaseFieldComparator : public Noncopyable {
 public:
  virtual bool compare(const RowID& left, const RowID& right) const = 0;
  virtual ~BaseFieldComparator() = default;
};