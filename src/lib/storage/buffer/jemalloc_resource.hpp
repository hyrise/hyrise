#pragma once

#ifdef HYRISE_WITH_JEMALLOC
#include <boost/container/pmr/memory_resource.hpp>
#include <utils/singleton.hpp>

namespace hyrise {
class JemallocMemoryResource : public boost::container::pmr::memory_resource, public Singleton<JemallocMemoryResource> {
  friend Singleton;
  friend struct ExtentHooks;

 public:
  JemallocMemoryResource();
  ~JemallocMemoryResource();

  void* do_allocate(std::size_t bytes, std::size_t alignment);
  void do_deallocate(void* pointer, std::size_t bytes, std::size_t alignment);
  bool do_is_equal(const boost::container::pmr::memory_resource& other) const noexcept;

 private:
  unsigned int _arena_index;
  int _mallocx_flags;
};
}  // namespace hyrise
#endif