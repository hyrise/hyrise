#pragma once

namespace hyrise {
/**
 * The BufferManagerAllocationObserver is used to track the allocation and deallocation of pages. A shared_ptr to the object can be registered at a 
 * BufferPoolAllocator using the register_observer method. Check AllocatorPinGuard for an example.
*/
class BufferManagerAllocationObserver {
 public:
  virtual void on_allocate(const void* ptr) = 0;
  virtual void on_deallocate(const void* ptr) = 0;
  virtual ~BufferManagerAllocationObserver() = default;
};
}  // namespace hyrise