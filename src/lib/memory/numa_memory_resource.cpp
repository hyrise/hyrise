#include "numa_memory_resource.hpp"

#include <sys/mman.h>
#include <boost/container/pmr/memory_resource.hpp>
#include "utils/assert.hpp"
#include "hyrise.hpp"

namespace {

// from  internals (see arena_types.h)
struct arena_config_s {
  /* extent hooks to be used for the arena */
  extent_hooks_t* extent_hooks;
};
using arena_config_t = struct arena_config_s;
}  // namespace

namespace hyrise {

NumaMemoryResource::NumaMemoryResource(const NodeID node_id) : _node_id(node_id) {
  // Setup  arena.
  auto arena_id = uint32_t{0};
  size_t size = sizeof(arena_id);
  Assert(mallctl("arenas.create", static_cast<void*>(&arena_id), &size, nullptr, 0) == 0, "mallctl failed");

  // In cases, std::vector deconstructs Memory Resources and constructs them again, due to memory management.
  // It can be the case, that at this point in time the hooks are not present.
  auto hooks_ptr = Hyrise::get().storage_manager.get_extent_hooks();
  if (hooks_ptr) {
    char command[64];
    snprintf(command, sizeof(command), "arena.%u.extent_hooks", arena_id);
    Hyrise::get().storage_manager.store_node_id_for_arena(arena_id, _node_id);
    Assert(mallctl(command, nullptr, nullptr, static_cast<void*>(&hooks_ptr), sizeof(extent_hooks_t*)) == 0,
          "mallctl failed");
    _allocation_flags = MALLOCX_ARENA(arena_id) | MALLOCX_TCACHE_NONE;
  }
}

void* NumaMemoryResource::do_allocate(std::size_t bytes, std::size_t alignment) {
  const auto addr = mallocx(bytes, _allocation_flags);
  return addr;
}

void NumaMemoryResource::do_deallocate(void* pointer, std::size_t bytes, std::size_t alignment) {
  dallocx(pointer, _allocation_flags);
}

bool NumaMemoryResource::do_is_equal(const memory_resource& other) const noexcept {
  return &other == this;
}

}  // namespace hyrise
