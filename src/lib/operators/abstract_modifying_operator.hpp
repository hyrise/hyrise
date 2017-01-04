#pragma once

#include <cstdint>

namespace opossum {

class AbstractModifyingOperator {
 public:
  virtual void commit(const uint32_t cid) = 0;
  virtual void abort() = 0;
};

}  // namespace opossum
