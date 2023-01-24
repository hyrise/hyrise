#pragma once

#include <cstddef>
#include <cstdint>
#include "buffer_managed_ptr.hpp"
#include "hyrise.hpp"
#include "types.hpp"

namespace hyrise {

//https://stackoverflow.com/questions/38010544/polymorphic-allocator-when-and-why-should-i-use-it
// http://www.club.cc.cmu.edu/~ajo/disseminate/2018-05-07-slides.pdf
//http://www.club.cc.cmu.edu/~ajo/disseminate/2018-05-07-slides.pdf
class BufferPoolResource {
 public:
  BufferManagedPtr<void> allocate(std::size_t bytes, std::size_t align = alignof(std::max_align_t)) {
    Assert(bytes <= PAGE_SIZE, "Cannot allocate more than a Page currently");
    auto page_id = buffer_manager().new_page();
    return BufferManagedPtr<void>(page_id, 0);
  }

  void deallocate(BufferManagedPtr<void> ptr, std::size_t bytes, std::size_t align = alignof(std::max_align_t)) {
    // do_deallocate(ptr, bytes, align);
  }

  bool is_equal(const BufferPoolResource& rhs) const noexcept {
    return false;
  }

  virtual ~BufferPoolResource() = default;

 protected:
  // virtual PointerType do_allocate(std::size_t bytes, std::size_t align) = 0;
  // virtual void do_deallocate(PointerType ptr, std::size_t bytes, std::size_t align) = 0;
  // virtual bool do_is_equal(const BufferPoolResource& rhs) const noexcept = 0;

 private:
  static BufferManager& buffer_manager() {
    return Hyrise::get().buffer_manager;
  }
};
}  // namespace hyrise