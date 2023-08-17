#pragma once

#include <jemalloc/jemalloc.h>
#include <boost/container/pmr/memory_resource.hpp>

#include "types.hpp"

namespace hyrise {

using ArenaID = uint32_t;

namespace numa_extent_hooks {

/**
 * Allocates bytes on a specific node.
 *
 * Similar to the behavior, that libnuma employs for numa_alloc_onnode(..).
 * First, allocate memory with mmap and then use numa_tonode_memory(..) to move
 * the allocated memory to a specific node.
 */
void* alloc(extent_hooks_t* extent_hooks, void* new_addr, size_t size, size_t alignment, bool* zero, bool* commit,
            unsigned arena_index);
bool dalloc(extent_hooks_t* extent_hooks, void* addr, size_t size, bool committed, unsigned arena_ind);

NodeID get_node_id_of_arena(ArenaID arena_index);
void store_node_id_for_arena(ArenaID arena_id, NodeID node_id);

};  // namespace numa_extent_hooks

class NumaMemoryResource : public boost::container::pmr::memory_resource {
 public:
  /**
   * Create for each NumaMemoryResource exactly one jemalloc arena.
   * We then use Extent Hooks, to customize the (de)allocation behavior,
   * where we use libnuma to allocate on a specific node.
   */
  explicit NumaMemoryResource(const NodeID node_id);
  void* do_allocate(std::size_t bytes, std::size_t alignment) override;
  void do_deallocate(void* p, std::size_t bytes, std::size_t alignment) override;
  bool do_is_equal(const memory_resource& other) const noexcept override;

 protected:
  NodeID _node_id;
  extent_hooks_t _hooks{};
  int32_t _allocation_flags;
};

}  // namespace hyrise
