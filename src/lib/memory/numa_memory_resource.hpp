#pragma once
#include <jemalloc/jemalloc.h>
#include <stdio.h>
#include <sys/mman.h>
#include <cstdint>
#include <unordered_map>
namespace hyrise {
using ArenaID = uint32_t;
class NumaExtentHooks {
 public:
  NumaExtentHooks() = delete;
  NumaExtentHooks(const NodeID node_id);

  static void* alloc(extent_hooks_t* extent_hooks, void* new_addr, size_t size, size_t alignment, bool* zero,
                     bool* commit, unsigned arena_index);
  static bool dalloc(extent_hooks_t* extent_hooks, void* addr, size_t size, bool committed, unsigned arena_ind);

  static void store_node_id_for_arena(ArenaID, NodeID);
 private:
  static std::unordered_map<ArenaID, NodeID> node_id_for_arena_id;
  
};
class NumaMemoryResource : public boost::container::pmr::memory_resource {
 public:
  NumaMemoryResource(const NodeID node_id);
  void* do_allocate(std::size_t bytes, std::size_t alignment) override;
  void do_deallocate(void* p, std::size_t bytes, std::size_t alignment) override;
  bool do_is_equal(const memory_resource& other) const noexcept override;

 protected:
  NodeID _node_id{0};
  extent_hooks_t _hooks{
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
  };
  int32_t _allocation_flags{0};
};

}  // namespace hyrise