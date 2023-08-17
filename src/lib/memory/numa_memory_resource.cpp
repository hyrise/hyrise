#include "numa_memory_resource.hpp"

#include <numa.h>
#include <sys/mman.h>
#include <unistd.h>

#include <shared_mutex>

#include "utils/assert.hpp"

namespace hyrise {

const auto PAGE_SIZE = getpagesize();

static auto node_id_for_arena_id = std::unordered_map<ArenaID, NodeID>{};
static auto node_id_for_arena_id_mutex = std::shared_mutex{};

namespace numa_extent_hooks {

void* alloc(extent_hooks_t* extent_hooks, void* new_addr, size_t size, size_t alignment, bool* zero, bool* commit,
            unsigned arena_index) {
  auto off = size_t{size % PAGE_SIZE};
  if (off > 0) {
    size += PAGE_SIZE - off;
  }

  auto addr = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  DebugAssert(addr != nullptr, "Failed to mmap pages.");

  auto node_id = get_node_id_of_arena(arena_index);
  numa_tonode_memory(addr, size, node_id);
  return addr;
}

NodeID get_node_id_of_arena(ArenaID arena_id) {
  std::shared_lock<std::shared_mutex> lock(node_id_for_arena_id_mutex);

  DebugAssert(node_id_for_arena_id.contains(arena_id), "Tried allocation for arena without numa node assignment.");

  return node_id_for_arena_id[arena_id];
}

void store_node_id_for_arena(ArenaID arena_id, NodeID node_id) {
  std::unique_lock<std::shared_mutex> lock(node_id_for_arena_id_mutex);

  if (node_id_for_arena_id.contains(arena_id)) {
    Fail("Tried to assign node id to an already assigned arena id.");
  }

  node_id_for_arena_id[arena_id] = node_id;
}

}  // namespace numa_extent_hooks

NumaMemoryResource::NumaMemoryResource(const NodeID node_id) : _node_id(node_id) {
  // Setup arena.
  auto arena_id = uint32_t{0};
  auto size = sizeof(arena_id);
  Assert(mallctl("arenas.create", static_cast<void*>(&arena_id), &size, nullptr, 0) == 0, "mallctl failed");

  // Add extent hooks to arena.
  _hooks.alloc = numa_extent_hooks::alloc;

  char command[64];
  snprintf(command, sizeof(command), "arena.%u.extent_hooks", arena_id);
  numa_extent_hooks::store_node_id_for_arena(arena_id, _node_id);
  // Apparently, jemalloc wants a pointer to the pointer to the struct.
  auto hooks_ptr = &_hooks;
  Assert(mallctl(command, nullptr, nullptr, static_cast<void*>(&hooks_ptr), sizeof(extent_hooks_t*)) == 0,
         "mallctl failed");
  _allocation_flags = MALLOCX_ARENA(arena_id) | MALLOCX_TCACHE_NONE;
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
