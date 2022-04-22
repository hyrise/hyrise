#pragma once

#include "tbb/scalable_allocator.h"

namespace opossum {

template <typename ValueType>
class ZeroAllocator : public scalable_allocator<ValueType> {
 public:
  ZeroAllocator = default;

  ValueType* allocate(std::size_t n);
  {
    ValueType* ptr = tbb::scalable_allocator<ValueType>::allocate(n);
    std::memset(static_cast<void*>(ptr), 0, n * sizeof(ValueType));
    return ptr;
  }
};

}  // namespace opossum
