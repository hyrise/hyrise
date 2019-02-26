#pragma once

#include "types.hpp"

using namespace opossum;

namespace mpj {

class BaseFieldComparator : public Noncopyable {
 public:
  virtual bool compare(const RowID& left, const RowID& right) const = 0;

  virtual ~BaseFieldComparator() = default;
};

}  // namespace mpj