#include "jemalloc_resource.hpp"
#ifdef HYRISE_WITH_JEMALLOC
#include <jemalloc/jemalloc.h>
#include <cstddef>
#include "hyrise.hpp"
#include "utils/assert.hpp"

namespace {

struct arena_config_s {
  extent_hooks_t* extent_hooks;
  bool metadata_use_hooks;
};

using arena_config_t = struct arena_config_s;

}  // namespace

namespace hyrise {

static void* extent_alloc(extent_hooks_t* extent_hooks, void* new_addr, size_t size, size_t alignment, bool* zero,
                          bool* commit, unsigned arena_index) {
  if (size > bytes_for_size_type(MAX_PAGE_SIZE_TYPE)) {
    // We cannot handle allocations larger than the largest PageSizeType. This ok because we only use jemalloc for
    // allocations of PageSizeType or smaller.
    return nullptr;
  }
  return Hyrise::get().buffer_manager.allocate(size, alignment);
}

bool extent_dalloc(extent_hooks_t* extent_hooks, void* addr, size_t size, bool committed, unsigned arena_ind) {
  // An extent deallocation function conforms to the extent_dalloc_t type and deallocates an extent at given addr
  // and size with committed/decommited memory as indicated, on behalf of arena arena_ind, returning false upon success.
  // If the function returns true, this indicates opt-out from deallocation;
  // the virtual memory mapping associated with the extent remains mapped, in the same commit state, and available for
  // future use, in which case it will be automatically retained for later reuse.
  Hyrise::get().buffer_manager.deallocate(addr, size);
  return true;
}

static void extent_destroy(extent_hooks_t* extent_hooks, void* addr, size_t size, bool committed, unsigned arena_ind) {}

static bool extent_commit(extent_hooks_t* extent_hooks, void* addr, size_t size, size_t offset, size_t length,
                          unsigned arena_ind) {
  return false;
}

static bool extent_purge_lazy(extent_hooks_t* extent_hooks, void* addr, size_t size, size_t offset, size_t length,
                              unsigned arena_ind) {
  return false;
}

static bool extent_purge(extent_hooks_t* extent_hooks, void* addr, size_t size, size_t offset, size_t length,
                         unsigned arena_ind) {
  return false;
}

static bool extent_split(extent_hooks_t* /*extent_hooks*/, void* /*addr*/, size_t /*size*/, size_t /*sizea*/,
                         size_t /*sizeb*/, bool /*committed*/, unsigned /*arena_ind*/) {
  return false;
}

static bool extent_merge(extent_hooks_t* /*extent_hooks*/, void* /*addra*/, size_t /*sizea*/, void* /*addrb*/,
                         size_t /*sizeb*/, bool /*committed*/, unsigned /*arena_ind*/) {
  return false;
}

static extent_hooks_t s_hooks{extent_alloc,      extent_dalloc, extent_destroy, extent_commit, nullptr,
                              extent_purge_lazy, extent_purge,  extent_split,   extent_merge};

JemallocMemoryResource::JemallocMemoryResource() {
  size_t size = sizeof(_arena_index);
  arena_config_t arena_config;
  arena_config.metadata_use_hooks = false;
  arena_config.extent_hooks = &s_hooks;

  Assert(mallctl("experimental.arenas_create_ext", static_cast<void*>(&_arena_index), &size, &arena_config,
                 sizeof(arena_config)) == 0,
         "arenas_create_ext failed");

  //   ssize_t muzzy_decay_ms = 1;
  //   auto muzzy_decay_cmd = "arena." + std::to_string(_arena_index) + ".muzzy_decay_ms";
  //   Assert(mallctl(muzzy_decay_cmd.c_str(), nullptr, nullptr, (void*)&muzzy_decay_ms, sizeof(muzzy_decay_cmd)) == 0,
  //          "setting muzzy_decay_ms failed");

  ssize_t dirty_decay_ms = -1;
  auto dirty_decay_cmd = "arena." + std::to_string(_arena_index) + ".dirty_decay_ms";
  Assert(mallctl(dirty_decay_cmd.c_str(), nullptr, nullptr, (void*)&dirty_decay_ms, sizeof(dirty_decay_ms)) == 0,
         "setting dirty_decay_ms failed");

  // TODO: Maybe add more arenas to reduce contention
  // retain_grow_limit? thread.arena
  _mallocx_flags = MALLOCX_ARENA(_arena_index) | MALLOCX_TCACHE_NONE;
}

JemallocMemoryResource::~JemallocMemoryResource() {}

void* JemallocMemoryResource::do_allocate(std::size_t bytes, std::size_t alignment) {
  // TODO:: uint32_t arena_idx = tk_thread_get_arena();
  if (auto ptr = mallocx(bytes, _mallocx_flags)) {
    return ptr;
  }
  Fail("Failed to allocate memory");
}

void JemallocMemoryResource::do_deallocate(void* pointer, std::size_t bytes, std::size_t alignment) {
  dallocx(pointer, _mallocx_flags);
}

bool JemallocMemoryResource::do_is_equal(const memory_resource& other) const noexcept {
  return &other == this;
}

}  // namespace hyrise

#endif