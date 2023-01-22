#include "buffer_pool_allocator.hpp"

namespace hyrise {
value_type* BufferPoolAllocator<T>::allocate(const size_t n) {}

void BufferPoolAllocatordeallocate(value_type* const ptr, size_t) {}
}  // namespace hyrise