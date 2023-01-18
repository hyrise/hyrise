#include <cstddef>
#include "types.h"

template <typename PointedType>
class BufferManagedPtr {
  using Allocator = std::allocator<PointedType>;

  void pin();
  void unpin(bool dirty = false);

 private:
  PageID _pageId;
  std::ptrdiff_t _offset;
  // ref_count is stored in buffer manager
  // PointedType *pointer;
};

template <class PointedType, class... Args>
BufferManagedPtr<PointedType> allocate_buffer_managed(Args&&... args) {
    return BufferManagedPtr<PointedType>();
}