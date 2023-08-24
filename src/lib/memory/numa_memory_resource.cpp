#include "numa_memory_resource.hpp"

#include <shared_mutex>

#ifdef HYRISE_WITH_JEMALLOC

#include <jemalloc/jemalloc.h>

#endif

#include <boost/container/pmr/memory_resource.hpp>

#include "hyrise.hpp"
#include "utils/assert.hpp"

namespace hyrise {

NumaMemoryResource::NumaMemoryResource(const NodeID node_id) : _node_id(node_id) {

#ifdef HYRISE_WITH_JEMALLOC
  // Setup arena.
  auto arena_id = uint32_t{0};
  auto size = sizeof(arena_id);
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
#endif
}

void* NumaMemoryResource::do_allocate(std::size_t bytes, std::size_t alignment) {
#ifdef HYRISE_WITH_JEMALLOC
  const auto addr = mallocx(bytes, _allocation_flags);
  return addr;
#else
  return malloc(bytes);
#endif
}

void NumaMemoryResource::do_deallocate(void* pointer, std::size_t bytes, std::size_t alignment) {
#ifdef HYRISE_WITH_JEMALLOC
  dallocx(pointer, _allocation_flags);
#else
  free(pointer);
#endif
}

bool NumaMemoryResource::do_is_equal(const memory_resource& other) const noexcept {
  return &other == this;
}

}  // namespace hyrise
