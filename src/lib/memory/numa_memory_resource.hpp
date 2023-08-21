#pragma once

#include <jemalloc/jemalloc.h>
#include <boost/container/pmr/memory_resource.hpp>

#include "types.hpp"

namespace hyrise {

using ArenaID = uint32_t;

/**
 * The base memory resource for NUMA memory allocation.
 * 
 * Each NumaMemoryResource is intended to correspond to exactly one NUMA node,
 * for which it creates a jemalloc arena to allocate memory with it. 
 * The NUMA specific allocation behavior is defined by extent_hooks, which are
 * located in the StorageManager. extent_hooks are a way of hooking into the 
 * allocation behavior of jemalloc and allow to use NUMA calls to move allocated 
 * memory to specific NUMA nodes.
 */
class NumaMemoryResource : public boost::container::pmr::memory_resource {
 public:
  // Constructor creating an arena for a specific node.
  explicit NumaMemoryResource(const NodeID node_id);

  // Methods defined by memory_resource.
  void* do_allocate(std::size_t bytes, std::size_t alignment) override;
  /**
   * Entry point for deallocation behavior.
   */
  void do_deallocate(void* p, std::size_t bytes, std::size_t alignment) override;
  bool do_is_equal(const memory_resource& other) const noexcept override;

 protected:
  NodeID _node_id{0};
  int32_t _allocation_flags{0};
};

}  // namespace hyrise
