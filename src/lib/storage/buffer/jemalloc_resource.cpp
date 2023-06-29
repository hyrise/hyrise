#include "jemalloc_resource.hpp"
#ifdef HYRISE_WITH_JEMALLOC
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

struct ResourceExtentHooks {
  static void* alloc(extent_hooks_t* extent_hooks, void* new_addr, size_t size, size_t alignment, bool* zero,
                     bool* commit, unsigned arena_index) {
    return Hyrise::get().buffer_manager.do_allocate(size, alignment);
  }

  // static bool dalloc(extent_hooks_t* extent_hooks, void* ptr, size_t size, bool committed, unsigned arena_index) {
  //   Hyrise::get().buffer_manager.do_deallocate(ptr, size, std::max_align_t);
  // }
};

JemallocMemoryResource::JemallocMemoryResource() {
  //  * JEMalloc holds pages in three states:
  //  *   - active: In use by the application
  //  *   - dirty: Held by JEMalloc for future allocations
  //  *   - muzzy: madvise(FREE) but not madvised(DONTNEED), so mapping may still
  //  *            exist, but kernel could reclaim if necessary
  //  * By default pages spend 10s in dirty state after being freed up, and then
  //  * move to muzzy state for an additional 10s prior to being
  //  * `madvise(DONTNEED)`.  This function reports the number of bytes that are in
  //  * the dirty state.  These are bytes unusable by the kernel, but also unused by
  //  * the application.  A force purge will make JEMalloc `madvise(DONTNEED)` these
  //  * pages immediately.
  //  *
  bool retain_enabled;
  size_t sz = sizeof(retain_enabled);
  // Assert(mallctl("opt.retain", &retain_enabled, &sz, NULL, 0), 0, "Unexpected mallctl() failure");
  _hooks.alloc = ResourceExtentHooks::alloc;
  size_t size = sizeof(_arena_index);
  arena_config_t arena_config;
  arena_config.metadata_use_hooks = false;
  arena_config.extent_hooks = &_hooks;

  Assert(mallctl("experimental.arenas_create_ext", static_cast<void*>(&_arena_index), &size, &arena_config,
                 sizeof(arena_config)) == 0,
         "arenas_create_ext failed");

  // ssize_t muzzy_decay_ms = -1;
  // auto muzzy_decay_cmd = "arena." + std::to_string(_arena_index) + ".muzzy_decay_ms";
  // Assert(mallctl(muzzy_decay_cmd.c_str(), nullptr, nullptr, (void*)&muzzy_decay_ms, sizeof(muzzy_decay_cmd)) == 0,
  //        "setting muzzy_decay_ms failed");

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
  return mallocx(bytes, _mallocx_flags);
}

void JemallocMemoryResource::do_deallocate(void* pointer, std::size_t bytes, std::size_t alignment) {
  dallocx(pointer, _mallocx_flags);
}

bool JemallocMemoryResource::do_is_equal(const memory_resource& other) const noexcept {
  return &other == this;
}

}  // namespace hyrise

#endif