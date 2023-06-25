#pragma once

#include "storage/buffer/frame.hpp"

namespace hyrise {
/**
 * The BufferPoolAllocatorObserver is used to track the allocation and deallocation of pages. A shared_ptr to the object can be registered at a 
 * BufferPoolAllocator using the register_observer method. Check AllocatorPinGuard for an example.
*/
class BufferPoolAllocatorObserver {
 public:
  virtual void on_allocate(const PageID page_id) = 0;
  virtual void on_deallocate(const PageID page_id) = 0;
  virtual ~BufferPoolAllocatorObserver() = default;
};
}  // namespace hyrise