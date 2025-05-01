#pragma once

#include <oneapi/tbb/cache_aligned_allocator.h>

namespace hyrise {

template <typename ValueType>
class ZeroAllocator : public tbb::cache_aligned_allocator<ValueType> {
 public:
  ZeroAllocator() = default;

  template <typename U>
  explicit ZeroAllocator(const ZeroAllocator<U>& /*unused*/) noexcept {}

  ValueType* allocate(std::size_t size) {
    ValueType* ptr = tbb::cache_aligned_allocator<ValueType>::allocate(size);
    std::memset(static_cast<void*>(ptr), 0, size * sizeof(ValueType));
    return ptr;
  }
};

}  // namespace hyrise
