#pragma once

#include <cstddef>
#include <cstdint>
#include "buffer_managed_ptr.hpp"
#include "page.hpp"
#include "storage/buffer/types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

class Hyrise;

// TODO: MOve this into buffer manager

//https://stackoverflow.com/questions/38010544/polymorphic-allocator-when-and-why-should-i-use-it
// http://www.club.cc.cmu.edu/~ajo/disseminate/2018-05-07-slides.pdf
//http://www.club.cc.cmu.edu/~ajo/disseminate/2018-05-07-slides.pdf
class BufferPoolResource {
 public:
  BufferManagedPtr<void> allocate(std::size_t bytes, std::size_t align = alignof(std::max_align_t));
  void deallocate(BufferManagedPtr<void> ptr, std::size_t bytes, std::size_t align = alignof(std::max_align_t));
};
}  // namespace hyrise