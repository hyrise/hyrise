#pragma once

#include <boost/container/pmr/memory_resource.hpp>

namespace hyrise {
#ifdef HYRISE_WITH_JEMALLOC
class JemallocMemoryResource : public boost::container::pmr::memory_resource {
 public:
  JemallocMemoryResource() {}

  void* do_allocate(std::size_t bytes, std::size_t alignment);
  void do_deallocate(void* pointer, std::size_t bytes, std::size_t alignment);
  bool do_is_equal(const boost::container::pmr::memory_resource& other) const noexcept;

 private:
  extent_hooks_t _hooks{};
  int _mallocx_flags;
};
#endif
}  // namespace hyrise