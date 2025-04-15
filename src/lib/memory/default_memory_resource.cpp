#include "default_memory_resource.hpp"

#include <cstddef>
#include <cstdlib>

#include "types.hpp"

namespace hyrise {

// We discourage manual memory management in Hyrise (such as malloc, or new), but in case of allocator/memory resource
// implementations, it is fine.
// NOLINTBEGIN(cppcoreguidelines-no-malloc,cppcoreguidelines-owning-memory,hicpp-no-malloc)

void* DefaultResource::do_allocate(std::size_t bytes, std::size_t /*alignment*/) {
  return std::malloc(bytes);
}

void DefaultResource::do_deallocate(void* pointer, std::size_t /*bytes*/, std::size_t /*alignment*/) {
  std::free(pointer);
}

[[nodiscard]] bool DefaultResource::do_is_equal(const memory_resource& other) const noexcept {
  return &other == this;
}
// NOLINTEND(cppcoreguidelines-no-malloc,cppcoreguidelines-owning-memory,hicpp-no-malloc)

}  // namespace hyrise
