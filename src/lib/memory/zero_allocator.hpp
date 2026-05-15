#pragma once

#include <oneapi/tbb/cache_aligned_allocator.h>

namespace hyrise {

template <typename ValueType>
class ZeroAllocator : public tbb::cache_aligned_allocator<ValueType> {
 public:
  ZeroAllocator() = default;

  template <typename U>
  explicit ZeroAllocator(const ZeroAllocator<U>& /*unused*/) noexcept {}

  ValueType* allocate(std::size_t count) {
    ValueType* ptr = tbb::cache_aligned_allocator<ValueType>::allocate(count);
    std::memset(static_cast<void*>(ptr), 0, count * sizeof(ValueType));
    return ptr;
  }
};

}  // namespace hyrise
