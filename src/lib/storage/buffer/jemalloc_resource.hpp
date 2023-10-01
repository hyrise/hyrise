#pragma once

#include <boost/container/pmr/memory_resource.hpp>
#include <memory>
#include <utils/singleton.hpp>
#include "allocation_observer.hpp"
#include "scheduler/topology.hpp"

namespace hyrise {

namespace detail {
static thread_local std::weak_ptr<BufferManagerAllocationObserver> _allocation_observer;
};

class JemallocBufferManagerProxyResource : public boost::container::pmr::memory_resource,
                                           public Singleton<JemallocBufferManagerProxyResource> {
  friend Singleton;

 public:
  JemallocBufferManagerProxyResource();
  ~JemallocBufferManagerProxyResource();

  void* do_allocate(std::size_t bytes, std::size_t alignment);
  void do_deallocate(void* pointer, std::size_t bytes, std::size_t alignment);
  bool do_is_equal(const boost::container::pmr::memory_resource& other) const noexcept;

  // Reset the arena to free all memory
  void reset();

  void configure_using_topology(const Topology& topology);

  void register_observer(const std::shared_ptr<BufferManagerAllocationObserver> observer);
  void deregister_observer(const std::shared_ptr<BufferManagerAllocationObserver> observer);

 private:
  unsigned int _arena_index;
  int _mallocx_flags;
  void create_arena();
};

static boost::container::pmr::memory_resource* get_default_jemalloc_memory_resource() {
  return &JemallocBufferManagerProxyResource::get();
}
}  // namespace hyrise
