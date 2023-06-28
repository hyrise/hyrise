#include "jemalloc_resource.hpp"
#ifdef HYRISE_WITH_JEMALLOC
#include <jemalloc/jemalloc.h>
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
    return Hyrise::get().do_allocate(size, alignment);
  }
};

JemallocMemoryResource::JemallocMemoryResource() {
  _hooks.alloc = ResourceExtentHooks::alloc;
  size_t size = sizeof(_arena_index);
  arena_config_t arena_config;
  arena_config.metadata_use_hooks = false;
  arena_config.extent_hooks = &_hooks;
    Assert(mallctl("experimental.arenas_create_ext", static_cast<void*>(&_arena_index), &size, &arena_config,
                    sizeof(arena_config)) == 0,
    _mallocx_flags = MALLOCX_ARENA(_arena_index) | MALLOCX_TCACHE_NONE;
}

void* JemallocMemoryResource::do_allocate(std::size_t bytes, std::size_t alignment) {
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