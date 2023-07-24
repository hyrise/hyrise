#pragma once

#include <boost/container/pmr/memory_resource.hpp>
#include <utils/singleton.hpp>

namespace hyrise {
class JemallocMemoryResource : public boost::container::pmr::memory_resource, public Singleton<JemallocMemoryResource> {
  friend Singleton;

 public:
  JemallocMemoryResource();
  ~JemallocMemoryResource();

  void* do_allocate(std::size_t bytes, std::size_t alignment);
  void do_deallocate(void* pointer, std::size_t bytes, std::size_t alignment);
  bool do_is_equal(const boost::container::pmr::memory_resource& other) const noexcept;

  void reset();

 private:
  unsigned int _arena_index;
  int _mallocx_flags;
  void create_arena();
};

static boost::container::pmr::memory_resource* get_default_jemalloc_memory_resource() {
  return &JemallocMemoryResource::get();
}
}  // namespace hyrise
