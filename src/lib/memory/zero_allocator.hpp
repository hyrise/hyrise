#pragma once

#include "tbb/cache_aligned_allocator.h"

namespace hyrise {

template <typename ValueType>
class ZeroAllocator : public tbb::cache_aligned_allocator<ValueType> {
 public:
  ZeroAllocator() = default;

  template <typename U>
  explicit ZeroAllocator(const ZeroAllocator<U>&) noexcept {}

  ValueType* allocate(std::size_t n) {
    ValueType* ptr = tbb::cache_aligned_allocator<ValueType>::allocate(n);
    std::memset(static_cast<void*>(ptr), 0, n * sizeof(ValueType));
    return ptr;
  }
};

}  // namespace hyrise
